package hostpath

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"context"

	"k8s.io/klog/v2"
	fs "k8s.io/kubernetes/pkg/volume/util/fs"
)

type LVM struct {
	vg       string
	thinpool string
}

func NewLVM(vg string, thinpool string) *LVM {
	return &LVM{
		vg:       vg,
		thinpool: thinpool,
	}
}

type reportWrapper struct {
	Reports []report `json:"report"`
}

type report struct {
	LogicalVolumes []reportLV `json:"lv"`
}

type reportLV struct {
	LogicalVolumeName string `json:"lv_name"`
	// VolumeGroupName   string `json:"vg_name"`
	// LogicalVolumeAttr string `json:"lv_attr"`
	LogicalVolumeSize string `json:"lv_size"`
	LogicalVolumeTags string `json:"lv_tags"`
	// PoolLogicalVolume string `json:"pool_lv"`
	// Origin            string `json:"origin"`
	// DataPercent       string `json:"data_percent"`
	// MetadataPercent   string `json:"metadata_percent"`
	// MovePV            string `json:"move_pv"`
	// MirrorLog         string `json:"mirror_log"`
	// CopyPercent       string `json:"copy_percent"`
	// ConvertLV         string `json:"convert_lv"`
}

func (r *LVMVolume) LogicalVolumeName() string {
	return r.info.LogicalVolumeName
}

func (r *LVMVolume) VolumeSizeBytes() (int64, error) {
	s := r.info.LogicalVolumeSize
	var multiplier int64
	if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
		multiplier = 1
	} else if strings.HasSuffix(s, "b") {
		s = strings.TrimSuffix(s, "b")
		multiplier = 1
	} else {
		return 0, fmt.Errorf("cannot parse size %q (unknown suffix)", s)
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing size %q: %w", r.info.LogicalVolumeSize, err)
	}
	n *= multiplier
	return n, nil
}

func (r *reportLV) FindTag(key string) (string, bool) {
	for _, kv := range strings.Split(r.LogicalVolumeTags, ",") {
		if strings.HasPrefix(kv, key+"=") {
			v := strings.TrimPrefix(kv, key+"=")
			return v, true
		}
	}
	return "", false
}

func runLVSReport(ctx context.Context, volumeName string) (*report, error) {
	args := []string{
		"--reportformat=json",
		"--options=lv_tags,lv_name,lv_size",
		"--units=b",
	}
	if volumeName != "" {
		args = append(args, volumeName)
	}
	c := exec.CommandContext(ctx, "/sbin/lvs", args...)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	var stderr bytes.Buffer
	c.Stderr = &stderr

	if err := c.Run(); err != nil {
		isNotFound := false
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode := exitError.ExitCode()
			if exitCode == 5 && strings.Contains(stderr.String(), "Failed to find logical volume") {
				isNotFound = true
			}
		}
		if volumeName == "" {
			// but we only get this error when we specify a non-existent LV
			isNotFound = false
		}
		if !isNotFound {
			return nil, fmt.Errorf("error running command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
		}
	}

	r := &reportWrapper{}
	if err := json.Unmarshal(stdout.Bytes(), r); err != nil {
		return nil, fmt.Errorf("error parsing output from command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
	}

	if len(r.Reports) != 1 {
		return nil, fmt.Errorf("error parsing output from command %v (stdout=%q, stderr=%q): got %d reports, expected exactly 1", c.Args, stdout.String(), stderr.String(), len(r.Reports))
	}
	return &r.Reports[0], nil
}

func (l *LVM) findVolumeByLVName(ctx context.Context, lvName string) (*LVMVolume, error) {
	info, err := findLVInfo(ctx, l.vg, lvName)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	volumePath := "/volumes/" + l.vg + "/" + lvName

	vol := &LVMVolume{
		volumePath: volumePath,
		info:       info,
	}

	if err := ensureMountLV(ctx, l.vg, vol.info.LogicalVolumeName, volumePath); err != nil {
		return nil, err
	}

	return vol, nil
}

type LVMVolume struct {
	volumePath string
	info       *reportLV
}

func (l *LVMVolume) GetVolumePath() string {
	return l.volumePath
}

type FSInfo struct {
	BytesAvailable int64
	BytesCapacity  int64
}

func (l *LVMVolume) GetFSInfo() (*FSInfo, error) {
	volumePath := l.GetVolumePath()
	fsInfo := &FSInfo{}
	var err error
	fsInfo.BytesAvailable, fsInfo.BytesCapacity, _, _, _, _, err = fs.Info(volumePath)
	if err != nil {
		return nil, err
	}
	return fsInfo, nil
}

func (l *LVM) findVolumeByVolumeID(ctx context.Context, volumeID string) (*LVMVolume, error) {
	// To avoid what (currently) seems like an unnecessary layer of indirection, volumeID == lvName
	lvName := volumeID

	return l.findVolumeByLVName(ctx, lvName)
}

func (l *LVM) createThinLV(ctx context.Context, lvName string, size string, tags []string) (*LVMVolume, error) {
	// Must precreate thinpool with: lvcreate -L 200G -T pool/thinpool
	// Can extend with e.g. /sbin/lvextend -L 20G pool/thinpool

	// WARNING: Check is skipped, please install recommended missing binary /usr/sbin/thin_check!
	// WARNING: Sum of all thin volume sizes (1.00 GiB) exceeds the size of thin pool pool/thinpool (200.00 MiB).
	// WARNING: You have not turned on protection against thin pools running out of space.
	// WARNING: Set activation/thin_pool_autoextend_threshold below 100 to trigger automatic extension of thin pools before they get full.
	// /usr/sbin/thin_check: execvp failed: No such file or directory
	// WARNING: Check is skipped, please install recommended missing binary /usr/sbin/thin_check!

	// lvcreate -V|--virtualsize Size[m|UNIT] --thinpool LV_thinpool VG
	// 	  [ -T|--thin ]
	// 	  [    --type thin ]
	// 	  [    --discards passdown|nopassdown|ignore ]
	// 	  [    --errorwhenfull y|n ]
	// 	  [ COMMON_OPTIONS ]

	args := []string{
		"--virtualsize", size,
		"--thinpool", l.thinpool,
		l.vg,
		"--thin",
		"--type", "thin",
		"--name", lvName,
	}
	for _, tag := range tags {
		args = append(args, "--addtag", tag)
	}
	c := exec.CommandContext(ctx, "/sbin/lvcreate", args...)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	var stderr bytes.Buffer
	c.Stderr = &stderr

	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("error running command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
	}

	if err := mkfsExt4(ctx, l.vg, lvName); err != nil {
		// TODO: Delete the LV
		return nil, fmt.Errorf("error formatting volume: %w", err)
	}
	lv, err := l.findVolumeByLVName(ctx, lvName)
	if err != nil {
		return nil, fmt.Errorf("error getting lv info for newly created volume: %w", err)
	}
	if lv == nil {
		return nil, fmt.Errorf("could not find LV info for newly created volume %q", lvName)
	}
	return lv, nil
}

func (l *LVM) deleteLV(ctx context.Context, volume *LVMVolume) error {
	if err := ensureUnmountLV(ctx, l.vg, volume.info.LogicalVolumeName, volume.volumePath); err != nil {
		return err
	}

	args := []string{
		"--yes",
		l.vg + "/" + volume.LogicalVolumeName(),
	}
	c := exec.CommandContext(ctx, "/sbin/lvremove", args...)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	var stderr bytes.Buffer
	c.Stderr = &stderr

	if err := c.Run(); err != nil {
		return fmt.Errorf("error running command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
	}

	return nil
}

func findLVInfo(ctx context.Context, vgName string, lvName string) (*reportLV, error) {
	report, err := runLVSReport(ctx, vgName+"/"+lvName)
	if err != nil {
		return nil, err
	}
	if len(report.LogicalVolumes) > 1 {
		return nil, fmt.Errorf("unexpectedly found multiple logical volumes, found %d", len(report.LogicalVolumes))
	}
	if len(report.LogicalVolumes) == 0 {
		return nil, nil
	}
	return &report.LogicalVolumes[0], nil
}

func ensureMountLV(ctx context.Context, vgName string, lvName string, mountPath string) error {
	lvPath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)

	if err := os.MkdirAll(mountPath, 0777); err != nil {
		return fmt.Errorf("error creating mount directory %q: %w", mountPath, err)
	}

	// --make-shared is required that this mount is visible outside this container.
	args := []string{"--make-shared", "-t", "ext4", lvPath, mountPath}
	c := exec.CommandContext(ctx, "/bin/mount", args...)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	var stderr bytes.Buffer
	c.Stderr = &stderr

	if err := c.Run(); err != nil {
		isAlreadyMounted := false
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode := exitError.ExitCode()
			if exitCode == 32 && strings.Contains(stderr.String(), "already mounted on") {
				isAlreadyMounted = true
				// TODO: Cache volumes
				klog.Infof("volume %q was already mounted", mountPath)
			}
		}

		if !isAlreadyMounted {
			return fmt.Errorf("error running command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
		}
	}

	return nil
}

func ensureUnmountLV(ctx context.Context, vgName string, lvName string, mountPath string) error {
	args := []string{mountPath}
	c := exec.CommandContext(ctx, "/bin/umount", args...)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	var stderr bytes.Buffer
	c.Stderr = &stderr

	if err := c.Run(); err != nil {
		return fmt.Errorf("error running command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
	}

	return nil
}

func mkfsExt4(ctx context.Context, vgName string, lvName string) error {
	devicePath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)

	label := lvName

	args := []string{"-L", label, devicePath}
	c := exec.CommandContext(ctx, "/sbin/mkfs.ext4", args...)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	var stderr bytes.Buffer
	c.Stderr = &stderr

	if err := c.Run(); err != nil {
		return fmt.Errorf("error running command %v (stdout=%q, stderr=%q): %w", c.Args, stdout.String(), stderr.String(), err)
	}

	return nil
}

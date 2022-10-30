# Copyright 2021 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.19.2 AS builder
WORKDIR /workspace
COPY go.mod go.sum /workspace/
RUN go mod download
ADD cmd /workspace/cmd/
ADD pkg /workspace/pkg/
ADD internal /workspace/internal/
RUN CGO_ENABLED=0 go build -o /workspace/lvmplugin ./cmd/lvmplugin

FROM debian:bullseye
# Add util-linux to get a new version of losetup.
# RUN apk add util-linux coreutils && apk update && apk upgrade
RUN apt-get update && apt-get install --yes lvm2 mount e2fsprogs
COPY --from=builder /workspace/lvmplugin /lvmplugin
ENTRYPOINT ["/lvmplugin"]

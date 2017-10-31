#!/bin/sh
rm -rf ./cmd/[^dO]*
go build -v -a ./cmd/dockerregistry/ 2>&1 | sed -e 's,^github.com/openshift/origin,.,' | tee build-cmd.log
go test -i -v -a ./pkg/dockerregistry/server/ 2>&1 | sed -e 's,^github.com/openshift/origin,.,' | tee build-test.log
sort -u build-cmd.log build-test.log | sed -n 'p; s,/vendor/k8s.io/,/vendor/k8s.io/kubernetes/staging/src/k8s.io/,p' >packages.txt
find ./pkg/ ./vendor -type d | while read -r pkg; do test -e "$pkg" || continue; grep -q -F "$pkg" packages.txt || rm -rv "$pkg"; done

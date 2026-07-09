FROM scratch
ARG TARGETOS
ARG TARGETARCH
LABEL org.opencontainers.image.title="Kubernetes Egress Forwarder" \
      org.opencontainers.image.description="Egress forwarder used by NetBird Operator to translate ports to destination ips and ports." \
      org.opencontainers.image.source="https://github.com/netbirdio/kube-egress-forwarder" \
      org.opencontainers.image.vendor="NetBird" \
      org.opencontainers.image.licenses="AGPL-3.0"
COPY bin/${TARGETOS}-${TARGETARCH}/kube-egress-forwarder /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["kube-egress-forwarder"]

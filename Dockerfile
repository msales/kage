FROM scratch

USER 12345

ADD dist/kage_docker /kage

ENV KAGE_ADDR ":80"

EXPOSE 80
CMD ["/kage"]
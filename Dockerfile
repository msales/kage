FROM scratch

ADD dist/kage_docker /kage

ENV KAGE_ADDR ":80"

EXPOSE 80
CMD ["/kage"]
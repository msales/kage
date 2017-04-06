FROM scratch

USER 12345

ADD dist/kage_docker /kage

CMD ["/kage"]
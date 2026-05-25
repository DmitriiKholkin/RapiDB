FROM alpine:3.20

RUN apk add --no-cache openssh-server shadow \
  && adduser -D -s /bin/sh rapidb_ssh \
  && echo 'rapidb_ssh:ssh_pass123' | chpasswd \
  && mkdir -p /var/run/sshd /home/rapidb_ssh/.ssh \
  && chown -R rapidb_ssh:rapidb_ssh /home/rapidb_ssh/.ssh \
  && ssh-keygen -A

COPY sshd_config /etc/ssh/sshd_config

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-e"]
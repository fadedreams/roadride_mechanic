FROM hashicorp/consul:1.15

RUN apk add --no-cache curl bash

COPY register.sh /register.sh
RUN chmod +x /register.sh

ENTRYPOINT ["/register.sh"]

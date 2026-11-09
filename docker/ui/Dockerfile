#
# conductor:ui - Conductor UI (legacy ui/, tenant)
#
FROM node:20-alpine
LABEL maintainer="Freshworks <search-team@freshworks.com>"

# Install the required packages for the node build
# to run on alpine
RUN apk update && apk add --no-cache python3 py3-pip make g++

WORKDIR /usr/src/app

COPY ./ui/package.json ./

RUN yarn install && mkdir -p public && cp -r node_modules/monaco-editor public/

COPY ./ui .

ENV REACT_APP_MONACO_EDITOR_USING_CDN=false
ENV REACT_APP_ENABLE_ERRORS_INSPECTOR=true
# Same-host ui switch (auth-proxy conductor-ui-variant cookie); off unless build-arg set.
ARG REACT_APP_UI_VARIANT_COOKIE_ENABLED=true
ENV REACT_APP_UI_VARIANT_COOKIE_ENABLED=${REACT_APP_UI_VARIANT_COOKIE_ENABLED}
CMD [ "yarn", "start" ]

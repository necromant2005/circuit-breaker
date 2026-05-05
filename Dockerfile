FROM ruby:3.3-alpine

RUN apk add --no-cache build-base

WORKDIR /app

COPY Gemfile Gemfile.lock ./
RUN bundle install

COPY config.ru ./
COPY bin ./bin
COPY lib ./lib

EXPOSE 8000

CMD ["ruby", "bin/server"]

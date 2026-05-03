FROM elixir:1.16-alpine

RUN apk add --no-cache build-base git

WORKDIR /app

ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
RUN mix local.hex --force && mix local.rebar --force && mix deps.get --only prod

COPY config ./config
COPY lib ./lib

RUN mix deps.compile && mix compile

EXPOSE 8000

CMD ["mix", "run", "--no-halt"]

ruff-check:
  uv tool run ruff check --fix

ruff-format:
  uv tool run ruff format

prek:
  prek run --all-files

install-hooks:
  uv tool run prek install -f --hook-type pre-commit

docker-up:
  docker compose up --build

docker-down:
  docker compose down -v

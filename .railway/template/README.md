# Материалы для страницы шаблона Railway

Заполняются руками в редакторе шаблона (Railway не даёт задать это через проект):

- `OVERVIEW.md` — Overview страницы, формат по гайду Railway.
- `VARIABLES.md` — описания переменных и флаг Required.
- `icon.png` — иконка 512×512 с прозрачным фоном.
- Category: **Starters**. Short description: «Пет-проект уровня Middle Go: три сервиса, gRPC, Kafka, Redis, Neon Postgres. romanovich.school».

Обновить уже опубликованный шаблон можно из CLI:

```bash
railway templates update <template-code> --category Starters --readme-file .railway/template/OVERVIEW.md \
  --description "Пет-проект уровня Middle Go: три сервиса, gRPC, Kafka, Redis, Neon Postgres. romanovich.school"
```

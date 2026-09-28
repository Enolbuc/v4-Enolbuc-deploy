// Package web вшивает фронтенд в бинарь gateway: сервис запускается из любого
// каталога и файловая система ему не нужна.
package web

import _ "embed"

//go:embed index.html
var Index []byte

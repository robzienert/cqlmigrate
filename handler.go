package cqlmigrate

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
)

// TODO: Make public
type handler interface {
	handle(name string, data string) error
}

type defaultHandler struct {
	driver   *driver
	override bool
}

func newDefaultHandler(driver *driver, override bool) *defaultHandler {
	return &defaultHandler{driver, override}
}

func (h *defaultHandler) handle(name string, data string) error {
	log.Printf("handling migration %s", name)
	md5, err := migrationMD5(data)
	if err != nil {
		return err
	}

	existingMD5, err := h.driver.getMigrationMD5(name)
	if err != nil {
		return err
	}
	if existingMD5 != "" {
		if md5 == existingMD5 {
			log.Printf("migration was already run: %s\n", name)
			return nil
		} else if !h.override {
			return fmt.Errorf("md5 of %s is different from the last time it was run", name)
		}
	} else {
		log.Printf("running migration: %s\n", name)
		err := h.driver.runMigration(Spec{name, data}, md5, h.override)
		if err != nil {
			return err
		}
	}
	return nil
}

func migrationMD5(data string) (string, error) {
	h := md5.New()
	if _, err := io.WriteString(h, data); err != nil {
		return "", fmt.Errorf("could not generate migration MD5: %s", err.Error())
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

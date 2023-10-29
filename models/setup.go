package models

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func ConnectDB(dsn string) error {
	database, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	err = database.AutoMigrate(&User{})
	if err != nil {
		return err
	}
	DB = database
	return nil
}

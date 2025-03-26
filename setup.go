package reconnect

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Model struct {
	gorm.Model

	Name string
}

func Setup(dsn string, name string) error {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	if err := db.AutoMigrate(&Model{}); err != nil {
		return err
	}

	if err := db.Create(&Model{Name: name}).Error; err != nil {
		return err
	}

	return nil
}

func SetupFirstDB() error {
	dsn := "host=127.0.0.1 user=user1 password=password1 dbname=db1 port=5432 sslmode=disable"
	name := "first-database"
	return Setup(dsn, name)
}

func SetupSecondDB() error {
	dsn := "host=127.0.0.1 user=user2 password=password2 dbname=db2 port=5433 sslmode=disable"
	name := "second-database"
	return Setup(dsn, name)
}

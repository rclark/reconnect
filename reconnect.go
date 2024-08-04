package reconnect

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Model struct {
	gorm.Model

	Name string
}

func Setup(dsn string, name string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&Model{}); err != nil {
		return nil, err
	}

	if err := db.Create(&Model{Name: name}).Error; err != nil {
		return nil, err
	}

	return db, nil
}

func SetupFirstDB() (*gorm.DB, error) {
	dsn := "host=127.0.0.1 user=user1 password=password1 dbname=db1 port=5432 sslmode=disable"
	name := "first-database"
	return Setup(dsn, name)
}

func SetupSecondDB() (*gorm.DB, error) {
	dsn := "host=127.0.0.1 user=user2 password=password2 dbname=db2 port=5433 sslmode=disable"
	name := "second-database"
	return Setup(dsn, name)
}

type Client struct {
	db *gorm.DB
}

func (u Client) Read() (Model, error) {
	var model Model
	err := u.db.First(&model).Error
	return model, err
}

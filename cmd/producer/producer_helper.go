package main

import (
	"errors"
	"fmt"
	"kafka-notify/models"
	"strconv"

	"github.com/gin-gonic/gin"
)

var ErrUserNotFoundInProducer = errors.New("user not found")

func findUserByID(id int) (models.User, error) {
	var user models.User
	if err := models.DB.Where("id=?", id).First(&user); err != nil {
		return user, nil
	}
	return models.User{}, ErrUserNotFoundInProducer
}

func getIDFromReqest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue))
	if err != nil {
		return 0, fmt.Errorf(
			"failed to parse ID from value %s: %w", formValue, err)
	}
	return id, nil
}

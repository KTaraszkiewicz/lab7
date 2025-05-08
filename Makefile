CC = gcc
CFLAGS = -lpthread -Wall -Wpedantic
TARGET = main

all: $(TARGET)

$(TARGET): main.c
	$(CC) $(CFLAGS) -o $(TARGET) main.c	-lm

clean:
	rm -f $(TARGET)
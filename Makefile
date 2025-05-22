CC = gcc
CFLAGS = -lpthread
TARGET = main

all: $(TARGET) lab8

lab8: lab8.c
	$(CC) $(CFLAGS) -o lab8 lab8.c	-lm 

$(TARGET): main.c
	$(CC) $(CFLAGS) -o $(TARGET) main.c	-lm

clean:
	rm -f $(TARGET)
	rm -f lab8
	rm -f *.out
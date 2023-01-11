package main

import (
	"bufio"
	"os"
	"strings"
)

func main() {
	for _, filePath := range []string{"dataset/test/wallets.tsv", "dataset/test/transactions.tsv"} {
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		fileScanner := bufio.NewScanner(file)
		fileScanner.Split(bufio.ScanLines)
		var lines []string
		for fileScanner.Scan() {
			lines = append(lines, strings.Join(strings.Fields(fileScanner.Text()), "\t"))
		}
		err = file.Truncate(0)
		if err != nil {
			panic(err)
		}
		_, err = file.Seek(0, 0)
		if err != nil {
			panic(err)
		}
		_, err = file.Write([]byte(strings.Join(lines, "\n")))
		if err != nil {
			panic(err)
		}
		err = file.Close()
		if err != nil {
			panic(err)
		}
	}
}

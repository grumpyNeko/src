package base

import "fmt"

func SSTableFileName(fn int) string {
	return fmt.Sprintf("%d.sst", fn)
}

func MetaFileName(fn int) string {
	return fmt.Sprintf("%d.meta", fn)
}

func LogFileName(fn int) string {
	return fmt.Sprintf("%d.log", fn)
}

func InputFileName(fn int) string {
	return fmt.Sprintf("%d.input", fn)
}
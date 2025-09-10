package datacollection

import (
	"github.com/nxadm/tail"
)

func TailLog(filePath string) (*tail.Tail, error) {
	t, err := tail.TailFile(filePath, tail.Config{Follow: true, ReOpen: true})
	if err != nil {
		return nil, err
	}
	return t, nil
}

package apidAnalytics

import (
	_ "fmt"
	"os"
	"strings"
	"path/filepath"
)

func uploadDir(dir os.FileInfo) bool {
	// TODO: handle upload to UAP file by file
	completePath := filepath.Join(localAnalyticsStagingDir, dir.Name())
	log.Debug("Complete Path : %s", completePath)
	tenant, timestamp := splitDirName(dir.Name())
	date := getDateFromDirTimestamp(timestamp)
	log.Debug("tenant: %s | timestamp %s", tenant, date)
	//for _, file := range dir {
	//	//log.Debugf("t: %s , file: %s", t, file.Name())
	//	if file.IsDir() {
	//		handleUploadDirStatus(file, uploadDir(file))
	//	}
	//}
	return false
}

func splitDirName(dirName string) (string, string){
	s := strings.Split("dirName", "~")
	tenant := s[0]+"~"+s[1]
	timestamp := s[2]
	return  tenant, timestamp
}

func getDateFromDirTimestamp(timestamp string) (string){
	return  ""
}
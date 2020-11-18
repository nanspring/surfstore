package surfstore

import(
	"errors"
	"log"
)
type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	//panic("todo")
	for k,v := range m.FileMetaMap{
		(*serverFileInfoMap)[k] = v
	}
	log.Println("Print getFileInfoMap")
	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	//panic("todo")
	fileName := fileMetaData.Filename
	newVersion := fileMetaData.Version
	currentVersion := m.FileMetaMap[fileName].Version
	log.Println("UpdateFile: ",fileName,newVersion)
	if newVersion != currentVersion+1 {
		*latestVersion = currentVersion
		return errors.New("version mismatch")
	}else{
		*latestVersion = newVersion
		m.FileMetaMap[fileName] = FileMetaData{fileName,newVersion,fileMetaData.BlockHashList}
	}
	return nil
}

var _ MetaStoreInterface = new(MetaStore)

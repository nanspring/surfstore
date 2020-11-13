package surfstore

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	//panic("todo")
	serverFileInfoMap = &m.FileMetaMap
	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	//panic("todo")
	fileName := fileMetaData.Filename
	newVersion := fileMetaData.Version
	currentVersion := m.FileMetaMap[fileName].Version
	if newVersion != currentVersion+1 {
		latestVersion = &currentVersion
	}else{
		latestVersion = &newVersion
		m.FileMetaMap[fileName] = *fileMetaData
	}
	return nil
}

var _ MetaStoreInterface = new(MetaStore)

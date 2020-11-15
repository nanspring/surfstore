package surfstore

import (
	"fmt"
	"os"
	"path/filepath"
	"buffio"
	"sha256"
	"ioutil"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	//panic("todo")

	//create index.txt if not exit
	CreateIndex(client)

	serverFileInfoMap := new(map[string]FileMetaData) //new return a pointer
	succ := new(bool)
	client.GetFileInfoMap(succ, serverFileInfoMap)
	

}

/*
Create index file for client if not exist
*/
func CreateIndex(client RPCClient){
	indexPath := client.BaseDir + "/index.txt"
	_, err := os.Stat(indexPath)
	if os.IsNotExist(err){
		file, err := os.Create(indexPath)
		if err != nil{
			log.Println("create index file error: ",err)
			return
		}
		defer file.Close()
	}
}

/*
1. scan base dirctory, 
2. compute file hash list
3. compare with local index file
*/
func ScanCheckLocalIndex(client RPCClient){
	var fileNames []string
	var indexMap map[string]string

	root := client.BaseDir
	indexPath := client.BaseDir + "/index.txt"

	GetIndexMap(&indexMap, indexPath)

	//file walk of base direcotry
	var fileNameUpdate []string // keep a record of files that needs update
	err := filepath.Walk(root,
    func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		//check whether filename exist in index.txt
		if val, ok := indexMap[path]; ok {
			
			//if so, check whether hashlist is different
			hashlist := strings.Split(val,",")[1]
			current_hashlist := GetFileHashList(path,client.BlockSize)
			//if different, update indexMap and append filename that need to be changed
			if hashlist != current_hashlist{ 
				indexMap[path] = current_hashlist
				fileNameUpdate = append(fileNameUpdate,path)
			}

		}
		fmt.Println(path, info.Size())
		return nil
	})
	if err != nil {
		log.Println(err)
	}

	
}

/*
Update hashlist of files in index.txt that need to be changed
*/
func UpdateIndexFile(indexPath string, indexMap map[string], fileUpdate []string){
	if len(fileUpdate) == 0{
		return
	}

	input, err := ioutil.ReadFile(indexPath)
	if err != nil {
		log.Println("read index file error: ",err)
		return
	}
	lines := strings.Split(string(input), "\n")
	
	for i, line := range lines{
		for fileName := range fileUpdate{
			if strings.Contains(line, fileName){
				temp = strings.Split(line,",")
				temp
				lines[i] = 
			}
		}
	}
}

/*
calculate file hashlist
*/
func GetFileHashList(path string, blockSize int) hashList string{
	byteFile := ioutil.ReadFile(path) //this return a byte array
	size := len(byteFile)/blockSize+1
	hashList = make([]string,size)

	for i := 0; i < size; i++{
		if(i == size-1){ //if this is the last chunk, dont split the file
			block = byteFile
		}else{
			block, byteFile = byteFile[:blockSize], byteFile[blockSize]
		}
		
		hashList += Hash256(block)+" "
	}

	return hashList
}

func Hash256(block byte[]) hash_code string{
	h := sha256.New()
	h.Write(block)
	hash_code = hex.EncodeToString(h.Sum(nil))
	return hash_code
}

/*
Read index.txt to a map
*/
func GetIndexMap(indexMap *map[string]string, indexPath string){
	file, err := os.Open(indexPath)
	if err != nil {
		log.Println("info file read error: ",err)
	}
	defer file.Close()
	scanner := buffio.NewScanner(file)
	for scanner.Scan(){
		line := strings.Split(scanner.Text(),",")
		indexMap[line[0]] = line[2] //line[0] is "filename", line[1] is "hashlist"
	}
	if err := scanner.Err(); err != nil{
		log.Println("infomap scann error: ".err)
	}

}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}

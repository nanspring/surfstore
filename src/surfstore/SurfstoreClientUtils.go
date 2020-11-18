package surfstore

import (
	"fmt"
	"os"
	"path/filepath"
	"bufio"
	"crypto/sha256"
	"io/ioutil"
	"log"
	"strings"
	"encoding/hex"
	"strconv"
	"reflect"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	//panic("todo")
	indexPath := client.BaseDir + "/index.txt"
	localIndexMap := make(map[string]FileMetaData)

	//create index.txt if not exist
	CreateIndex(client)
	GetIndexMap(&localIndexMap, indexPath) // read index.txt to map

	//scan local file, check for any file modification or new file 
	fileNameUpdate := ScanCheckLocalIndex(localIndexMap,client)
	log.Println("fileNameUpdate: ",fileNameUpdate)


	serverFileInfoMap := new(map[string]FileMetaData) //new return a pointer
	succ := new(bool)
	client.GetFileInfoMap(succ, serverFileInfoMap) // rpc call, get index map from server
	PrintMetaMap(*serverFileInfoMap)

	//=========TODO=================

	// Compare local index with remote server index
	for file_name, file_meta_data := range *serverFileInfoMap {
		if _, ok := localIndexMap[file_name]; !ok { //if file does not exist in local index, server has new file :)
			//download file block from server
			hashlist := file_meta_data.BlockHashList
			var blockList []*Block
			// ====TODO====
			// ==Get block list from server by looping hashlist
			DownloadBlock(hashlist, blockList, client)
			// ==reorganized downloaded blocks into file into local dir
			JoinBlockAndDownloadFile(blockList, file_name, client)

			// ==update localIndexMap of the newly downloaded file
			localIndexMap[file_name] = FileMetaData{file_name,file_meta_data.Version,hashlist}
			fileNameUpdate[file_name] = true
		}
	}

	// client upload new file to server, if fail, delete filename in fileNameUpdate or newFileName
	for name,_ := range fileNameUpdate{
		var latestVersion = new(int)
		local_fmData := localIndexMap[name]
		err := client.UpdateFile(&local_fmData, latestVersion)
		if err != nil {
			// if error, meaning version mismatch. Download the file from server
			file_meta_data := (*serverFileInfoMap)[name]
			hashlist := file_meta_data.BlockHashList
			var blockList []*Block
			DownloadBlock(hashlist, blockList,client)
			JoinBlockAndDownloadFile(blockList, name, client)
			localIndexMap[name] = FileMetaData{name,*latestVersion,hashlist}

		}else{
			// upload to block of file to server
			blockList := GetFileBlock(name, client)

			for _, block := range blockList{
				err = client.PutBlock(block, succ)
				PrintError(err, "Put Block")
		}
		}

	}
	//==============================

	//Update the hashlist of corresponding file, rewrite index.txt
	UpdateIndexFile(indexPath, localIndexMap, fileNameUpdate)
	
	

}
/*
Download block
*/
func DownloadBlock(hashList []string, blockList []*Block, client RPCClient){
	for _,blockHash := range hashList{
		block := new(Block)
		err := client.GetBlock(blockHash, block)
		PrintError(err,"Get Block")
		blockList = append(blockList, block)
	}
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
4. return fileNames that need update
*/
func ScanCheckLocalIndex(indexMap map[string]FileMetaData, client RPCClient) (fileNameUpdate map[string]bool){

	root := client.BaseDir
	indexPath := root + "/index.txt"


	//file walk of base direcotry
	err := filepath.Walk(root,
    func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() { //skip current directory
			return nil
		}

		if path == indexPath { //ignore index.txt
			return nil
		}
		//get the current hashlist of the file
		current_hashlist := GetFileHashList(path,client.BlockSize,client)
		
		//check whether filename exist in index.txt
		if fmdata, ok := indexMap[path]; ok {
			
			
			//if different, update indexMap and append filename that need to be changed
			if !assertEq(fmdata.BlockHashList,current_hashlist){ 
				log.Println("hashlist different")
				fmdata.BlockHashList = current_hashlist
				fmdata.Version = fmdata.Version+1 //update version+1
				indexMap[path] = fmdata
				fileNameUpdate[path] = true
			}
			
		}else{ // if not exist in index.txt, it is a new file, append new line to index.txt
			fileNameUpdate[path] =true
			indexMap[path] = FileMetaData{path[len(root)+1:], 1, current_hashlist} 
			
			
		}
		
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	
	

	return fileNameUpdate
}

/*
Update hashlist of files in index.txt that need to be changed
Append new file in index.txt created by client
*/
func UpdateIndexFile(indexPath string, indexMap map[string]FileMetaData, fileUpdate map[string]bool){
	if len(fileUpdate) == 0{
		return
	}

	input, err := ioutil.ReadFile(indexPath)
	if err != nil {
		log.Println("read index file error: ",err)
		return
	}

	var lines []string

	if len(fileUpdate) != 0 {
		lines = strings.Split(string(input), "\n")
	
		for i, line := range lines{ //check whether each line contains file that need updates
			for fileName,_ := range fileUpdate{
				if strings.Contains(line, fileName){
					fmdata := indexMap[fileName] 
					data := []string {fmdata.Filename,strconv.Itoa(fmdata.Version),strings.Join(fmdata.BlockHashList," ")}
					lines[i] = strings.Join(data,",") //rejoin the string
			}
		}
	}

	}

	output := strings.Join(lines,"\n")
	err = ioutil.WriteFile(indexPath,[]byte(output),0644) // rewrite index.txt
	if err != nil{
		log.Println("write index file error: ",err)
	}

	
}

/*
calculate file hashlist
*/
func GetFileHashList(path string, blockSize int, client RPCClient) (hashList []string){
	byteFile, err := ioutil.ReadFile(client.BaseDir + "/" + path) //this return a byte array
	if err != nil{
		log.Println("error reading file: ",path,err)
		return hashList
	}

	size := len(byteFile)/blockSize+1
	block := make([]byte,0,size)
	
	for i := 0; i < size; i++{

		if(i == size-1){ //if this is the last chunk, dont split the file
			block = byteFile
		}else{
			block, byteFile = byteFile[:blockSize], byteFile[blockSize:]
		}
		
		hashList = append(hashList,Hash256(block))
	}

	return hashList
}

/*
Get block of file to be ready to upload to server
*/
func GetFileBlock(name string, client RPCClient)(blockList []Block){
	byteFile, err := ioutil.ReadFile(client.BaseDir + "/" + name) //this return a byte array
	if err != nil{
		log.Println("error reading file: ",name,err)
		return blockList
	}

	blockSize := client.BlockSize
	size := len(byteFile)/blockSize+1
	block := make([]byte,0,size)

	for i := 0; i < size; i++{

		if(i == size-1){ //if this is the last chunk, dont split the file
			block = byteFile[:len(byteFile)]
		}else{
			block, byteFile = byteFile[:blockSize], byteFile[blockSize:]
		}
		
		blockList = append(blockList,Block{block,len(block)})
	}

	return blockList

}

func Hash256(block []byte) (hash_code string){
	h := sha256.New()
	h.Write(block)
	hash_code = hex.EncodeToString(h.Sum(nil))
	return hash_code
}

/*
Read index.txt to a map
*/
func GetIndexMap(indexMap *map[string]FileMetaData, indexPath string){
	file, err := os.Open(indexPath)
	if err != nil {
		log.Println("info file read error: ",err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan(){
		line := strings.Split(scanner.Text(),",") //line[0] is "filename", line[1] is version, line[2] is "hashlist"
		version,_ := strconv.Atoi(line[1])
		fmdata := FileMetaData{line[0],version,strings.Split(line[2]," ")}
		(*indexMap)[line[0]] = fmdata 
	}
	if err := scanner.Err(); err != nil{
		log.Println("infomap scann error: ",err)
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

func JoinBlockAndDownloadFile(blockList []*Block, filename string, client RPCClient){
	// join blocks of file
	var result []byte
	for _, block := range blockList{
		result = append(result, ((*block).BlockData)...)
	}

	//create file
	path := client.BaseDir + "/" + filename
	_, err := os.Stat(path)
	if os.IsNotExist(err){
		file, err := os.Create(path)
		if err != nil{
			log.Println("create index file error: ",err)
			return
		}
		defer file.Close()
	}
	err = ioutil.WriteFile(path,result,0644)
}

func assertEq(test []string, ans []string) bool {
    return reflect.DeepEqual(test, ans)
}

func PrintError(err error, msg string){
	if err != nil{
		log.Println(msg, " error: ",err)
	}
}

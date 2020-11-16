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
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	//panic("todo")

	//create index.txt if not exist
	CreateIndex(client)

	//scan local file, check for any file modification or new file and rewrite index.txt
	ScanCheckLocalIndex(client)

	serverFileInfoMap := new(map[string]FileMetaData) //new return a pointer
	succ := new(bool)
	client.GetFileInfoMap(succ, serverFileInfoMap)
	PrintMetaMap(*serverFileInfoMap)
	

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
	var fileNameUpdate []string // keep a record of files that needs update
	var indexMap map[string]string

	root := client.BaseDir
	indexPath := root + "/index.txt"

	GetIndexMap(&indexMap, indexPath)

	//file walk of base direcotry
	err := filepath.Walk(root,
    func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		//get the current hashlist of the file
		current_hashlist := GetFileHashList(path,client.BlockSize)

		//check whether filename exist in index.txt
		if val, ok := indexMap[path]; ok {
			
			//if so, check whether hashlist is different
			hashlist := strings.Split(val,",")[1]
			
			//if different, update indexMap and append filename that need to be changed
			if hashlist != current_hashlist{ 
				indexMap[path] = current_hashlist
				fileNameUpdate = append(fileNameUpdate,path)
			}

		}else{ // if not exist in index.txt, it is a new file, append new line to index.txt
			text := path + "," + "1" + "," + current_hashlist
			f, err := os.OpenFile(indexPath, os.O_APPEND|os.O_WRONLY, 0644) 
			defer f.Close()
			_, err = f.WriteString(text) 
			if err != nil {
				log.Println("Append line to index error: ",err)
				return err
			}
			
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	
	//Update the hashlist of corresponding file 
	if len(fileNameUpdate) != 0 {
		UpdateIndexFile(indexPath, indexMap, fileNameUpdate)
	}

	
}

/*
Update hashlist of files in index.txt that need to be changed
*/
func UpdateIndexFile(indexPath string, indexMap map[string]string, fileUpdate []string){
	if len(fileUpdate) == 0{
		return
	}

	input, err := ioutil.ReadFile(indexPath)
	if err != nil {
		log.Println("read index file error: ",err)
		return
	}
	lines := strings.Split(string(input), "\n")
	
	for i, line := range lines{ //check whether each line contains file that need updates
		for _,fileName := range fileUpdate{
			if strings.Contains(line, fileName){
				temp := strings.Split(line,",") //split the line
				version, _:= strconv.Atoi(temp[1])
				temp[1] = strconv.Itoa(version+1) //update version+1
				temp[2] = indexMap[temp[0]] //update the hashlist
				lines[i] = strings.Join(temp,",") //rejoin the string
			}
		}
	}

	output := strings.Join(lines,"\n")
	err = ioutil.WriteFile("index.txt",[]byte(output),0644) // rewrite index.txt
	if err != nil{
		log.Println("write index file error: ",err)
	}
}

/*
calculate file hashlist
*/
func GetFileHashList(path string, blockSize int) (hashList string){
	byteFile, err := ioutil.ReadFile(path) //this return a byte array
	if err != nil{
		log.Println("error reading file: ",path,err)
		return hashList
	}
	size := len(byteFile)/blockSize+1

	var block []byte
	for i := 0; i < size; i++{
		if(i == size-1){ //if this is the last chunk, dont split the file
			block = byteFile
		}else{
			block, byteFile = byteFile[:blockSize], byteFile[blockSize:]
		}
		
		hashList += Hash256(block)+" "
	}

	return hashList
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
func GetIndexMap(indexMap *map[string]string, indexPath string){
	file, err := os.Open(indexPath)
	if err != nil {
		log.Println("info file read error: ",err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan(){
		line := strings.Split(scanner.Text(),",")
		(*indexMap)[line[0]] = line[2] //line[0] is "filename", line[1] is "hashlist"
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

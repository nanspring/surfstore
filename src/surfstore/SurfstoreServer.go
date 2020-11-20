package surfstore

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"errors"
	"fmt"
)

type Server struct {
	BlockStore BlockStoreInterface
	MetaStore  MetaStoreInterface
}

func (s *Server) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	//panic("todo")
	err := s.MetaStore.GetFileInfoMap(succ,serverFileInfoMap)
	return err
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	//panic("todo")
	err := s.MetaStore.UpdateFile(fileMetaData, latestVersion)
	return err
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	//panic("todo")
	err := s.BlockStore.GetBlock(blockHash, blockData)
	return err
}

func (s *Server) PutBlock(blockData Block, succ *bool) error {
	//panic("todo")
	err := s.BlockStore.PutBlock(blockData, succ)
	return err
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	//panic("todo")
	err := s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
	return err
}

// This line guarantees all method for surfstore are implemented
var _ Surfstore = new(Server)

func NewSurfstoreServer() Server {
	blockStore := BlockStore{BlockMap: map[string]Block{}}
	metaStore := MetaStore{FileMetaMap: map[string]FileMetaData{}}

	return Server{
		BlockStore: &blockStore,
		MetaStore:  &metaStore,
	}
}

func ServeSurfstoreServer(hostAddr string, surfstoreServer Server) error {
	//panic("todo")

	//register server to rpc and name the server as "Surfstore"
	rpc.RegisterName("Surfstore",&surfstoreServer)
	rpc.HandleHTTP()

	log.Println("Server Start Listening at : ",hostAddr)
	l, err := net.Listen("tcp",hostAddr)
	if err != nil {
		log.Println("listen error: ",err)
		return errors.New("listen error")
	}
	go http.Serve(l,nil)

	// fmt.Print("Press enter key to end server")
	// fmt.Scanln()
	// return errors.New("server stop")
}

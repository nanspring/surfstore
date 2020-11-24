package surfstore

import(
	"errors"
	"crypto/sha256"
	"encoding/hex"
)
type BlockStore struct {
	BlockMap map[string]Block
}

func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	//panic("todo")
	if block, ok := bs.BlockMap[blockHash] ; ok {
		blockData.BlockSize = block.BlockSize
		blockData.BlockData = make([]byte, blockData.BlockSize)
		copy(blockData.BlockData, block.BlockData)
		return nil
	}else{
		return errors.New("Block does not exist")
	}
}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	//panic("todo")
	h := sha256.New()
	h.Write(block.BlockData)
	hash_code := hex.EncodeToString(h.Sum(nil))
	bs.BlockMap[hash_code] = block
	return nil
}

func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	//panic("todo")
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

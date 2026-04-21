impl :: bincode :: Encode for WalEntry
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        match self
        {
            Self ::Write { address, revision, data }
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (0u32), encoder) ?
                ; :: bincode :: Encode :: encode(address, encoder) ?; ::
                bincode :: Encode :: encode(revision, encoder) ?; :: bincode
                :: Encode :: encode(data, encoder) ?; core :: result :: Result
                :: Ok(())
            }, Self ::Tombstone { address, revision }
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (1u32), encoder) ?
                ; :: bincode :: Encode :: encode(address, encoder) ?; ::
                bincode :: Encode :: encode(revision, encoder) ?; core ::
                result :: Result :: Ok(())
            }, Self ::BlockSealed { block_id, space, snapshot }
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (2u32), encoder) ?
                ; :: bincode :: Encode :: encode(block_id, encoder) ?; ::
                bincode :: Encode :: encode(space, encoder) ?; :: bincode ::
                Encode :: encode(snapshot, encoder) ?; core :: result ::
                Result :: Ok(())
            }, Self ::Checkpoint { revision }
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (3u32), encoder) ?
                ; :: bincode :: Encode :: encode(revision, encoder) ?; core ::
                result :: Result :: Ok(())
            },
        }
    }
}
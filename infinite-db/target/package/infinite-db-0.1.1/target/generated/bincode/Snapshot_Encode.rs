impl :: bincode :: Encode for Snapshot
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.id, encoder) ?; :: bincode ::
        Encode :: encode(&self.space, encoder) ?; :: bincode :: Encode ::
        encode(&self.revision, encoder) ?; :: bincode :: Encode ::
        encode(&self.parent, encoder) ?; :: bincode :: Encode ::
        encode(&self.blocks, encoder) ?; core :: result :: Result :: Ok(())
    }
}
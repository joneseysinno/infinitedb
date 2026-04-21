impl :: bincode :: Encode for Record
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.address, encoder) ?; :: bincode
        :: Encode :: encode(&self.revision, encoder) ?; :: bincode :: Encode
        :: encode(&self.data, encoder) ?; :: bincode :: Encode ::
        encode(&self.tombstone, encoder) ?; core :: result :: Result :: Ok(())
    }
}
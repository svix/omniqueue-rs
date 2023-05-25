use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::QueueError;

pub type DecoderRegistry<T> = Arc<HashMap<TypeId, Arc<dyn CustomDecoder<T>>>>;

pub trait CustomDecoder<P: Send + Sync>: Send + Sync {
    fn item_type(&self) -> TypeId;
    fn decode(&self, payload: &P) -> Result<Box<dyn Any + Send + Sync>, QueueError>;
}

pub struct CustomDecoderStandardized<P: Send + Sync, F: Fn(&Vec<u8>) -> Result<P, QueueError>> {
    decoder: Arc<dyn CustomDecoder<P>>,
    conversion: F,
}

/// A standardized decoder for `Vec<u8>` payloads
impl<P: 'static + Send + Sync, F: Fn(&Vec<u8>) -> Result<P, QueueError> + Send + Sync>
    CustomDecoder<Vec<u8>> for CustomDecoderStandardized<P, F>
{
    fn item_type(&self) -> TypeId {
        self.decoder.type_id()
    }

    fn decode(&self, payload: &Vec<u8>) -> Result<Box<dyn Any + Send + Sync>, QueueError> {
        let converted = (self.conversion)(payload)?;
        self.decoder.decode(&converted)
    }
}

impl<P: Send + Sync, F: Fn(&Vec<u8>) -> Result<P, QueueError> + Send + Sync>
    CustomDecoderStandardized<P, F>
{
    pub fn from_decoder(decoder: Arc<dyn CustomDecoder<P>>, conversion: F) -> Self {
        Self {
            decoder,
            conversion,
        }
    }
}

pub trait IntoCustomDecoder<I: Send + Sync, O: Send + Sync> {
    fn into(self) -> Arc<dyn CustomDecoder<I>>;
}

struct FnDecoder<
    I: 'static + Send + Sync,
    O: 'static + Send + Sync,
    F: 'static + Fn(&I) -> Result<O, QueueError> + Send + Sync,
> {
    f: F,

    _i_pd: PhantomData<I>,
    _o_pd: PhantomData<O>,
}

impl<
        I: 'static + Send + Sync,
        O: 'static + Send + Sync,
        F: 'static + Fn(&I) -> Result<O, QueueError> + Send + Sync,
    > CustomDecoder<I> for FnDecoder<I, O, F>
{
    fn item_type(&self) -> TypeId {
        TypeId::of::<O>()
    }

    fn decode(&self, payload: &I) -> Result<Box<dyn Any + Send + Sync>, QueueError> {
        Ok((self.f)(payload).map(Box::new)?)
    }
}

impl<
        I: 'static + Send + Sync,
        O: 'static + Send + Sync,
        F: 'static + Fn(&I) -> Result<O, QueueError> + Send + Sync,
    > IntoCustomDecoder<I, O> for F
{
    fn into(self) -> Arc<dyn CustomDecoder<I>> {
        Arc::new(FnDecoder {
            f: self,
            _i_pd: PhantomData,
            _o_pd: PhantomData,
        })
    }
}

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::Result;

pub type DecoderRegistry<T> = Arc<HashMap<TypeId, Arc<dyn CustomDecoder<T>>>>;

pub trait CustomDecoder<P>: Send + Sync {
    fn item_type(&self) -> TypeId;
    fn decode(&self, payload: &P) -> Result<Box<dyn Any + Send + Sync>>;
}

pub struct CustomDecoderStandardized<P, F> {
    decoder: Arc<dyn CustomDecoder<P>>,
    conversion: F,
}

/// A standardized decoder for `Vec<u8>` payloads
impl<P, F> CustomDecoder<Vec<u8>> for CustomDecoderStandardized<P, F>
where
    P: 'static,
    F: Fn(&Vec<u8>) -> Result<P> + Send + Sync,
{
    fn item_type(&self) -> TypeId {
        self.decoder.type_id()
    }

    fn decode(&self, payload: &Vec<u8>) -> Result<Box<dyn Any + Send + Sync>> {
        let converted = (self.conversion)(payload)?;
        self.decoder.decode(&converted)
    }
}

impl<P, F> CustomDecoderStandardized<P, F>
where
    F: Fn(&Vec<u8>) -> Result<P> + Send + Sync,
{
    pub fn from_decoder(decoder: Arc<dyn CustomDecoder<P>>, conversion: F) -> Self {
        Self {
            decoder,
            conversion,
        }
    }
}

pub trait IntoCustomDecoder<I, O> {
    fn into(self) -> Arc<dyn CustomDecoder<I>>;
}

struct FnDecoder<I, O, F> {
    f: F,

    _i_pd: PhantomData<I>,
    _o_pd: PhantomData<O>,
}

impl<I, O, F> CustomDecoder<I> for FnDecoder<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> Result<O> + Send + Sync + 'static,
{
    fn item_type(&self) -> TypeId {
        TypeId::of::<O>()
    }

    fn decode(&self, payload: &I) -> Result<Box<dyn Any + Send + Sync>> {
        Ok((self.f)(payload).map(Box::new)?)
    }
}

impl<I, O, F> IntoCustomDecoder<I, O> for F
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> Result<O> + Send + Sync + 'static,
{
    fn into(self) -> Arc<dyn CustomDecoder<I>> {
        Arc::new(FnDecoder {
            f: self,
            _i_pd: PhantomData,
            _o_pd: PhantomData,
        })
    }
}

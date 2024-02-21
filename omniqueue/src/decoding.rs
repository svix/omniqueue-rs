use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::Result;

pub type DecoderRegistry = Arc<HashMap<TypeId, Arc<dyn CustomDecoder>>>;

pub trait CustomDecoder: Send + Sync {
    fn item_type(&self) -> TypeId;
    fn decode(&self, payload: &str) -> Result<Box<dyn Any + Send + Sync>>;
}

pub struct CustomDecoderStandardized<F> {
    decoder: Arc<dyn CustomDecoder>,
    conversion: F,
}

impl<F> CustomDecoder for CustomDecoderStandardized<F>
where
    F: Fn(&str) -> Result<String> + Send + Sync,
{
    fn item_type(&self) -> TypeId {
        self.decoder.type_id()
    }

    fn decode(&self, payload: &str) -> Result<Box<dyn Any + Send + Sync>> {
        let converted = (self.conversion)(payload)?;
        self.decoder.decode(&converted)
    }
}

impl<F> CustomDecoderStandardized<F>
where
    F: Fn(&str) -> Result<String> + Send + Sync,
{
    pub fn from_decoder(decoder: Arc<dyn CustomDecoder>, conversion: F) -> Self {
        Self {
            decoder,
            conversion,
        }
    }
}

pub trait IntoCustomDecoder<O> {
    fn into(self) -> Arc<dyn CustomDecoder>;
}

struct FnDecoder<O, F> {
    f: F,

    _o_pd: PhantomData<O>,
}

impl<O, F> CustomDecoder for FnDecoder<O, F>
where
    O: Send + Sync + 'static,
    F: Fn(&str) -> Result<O> + Send + Sync + 'static,
{
    fn item_type(&self) -> TypeId {
        TypeId::of::<O>()
    }

    fn decode(&self, payload: &str) -> Result<Box<dyn Any + Send + Sync>> {
        Ok((self.f)(payload).map(Box::new)?)
    }
}

impl<O, F> IntoCustomDecoder<O> for F
where
    O: Send + Sync + 'static,
    F: Fn(&str) -> Result<O> + Send + Sync + 'static,
{
    fn into(self) -> Arc<dyn CustomDecoder> {
        Arc::new(FnDecoder {
            f: self,
            _o_pd: PhantomData,
        })
    }
}

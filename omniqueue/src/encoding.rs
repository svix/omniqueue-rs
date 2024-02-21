use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::{QueueError, Result};

pub type EncoderRegistry = Arc<HashMap<TypeId, Box<dyn CustomEncoder>>>;

pub trait CustomEncoder: Send + Sync {
    fn item_type(&self) -> TypeId;
    fn encode(&self, item: &(dyn Any + Send + Sync)) -> Result<String>;
}

pub trait IntoCustomEncoder<I> {
    fn into(self) -> Box<dyn CustomEncoder>;
}

struct FnEncoder<I, F> {
    f: F,

    _i_pd: PhantomData<I>,
}

impl<I, F> CustomEncoder for FnEncoder<I, F>
where
    I: Send + Sync + 'static,
    F: Fn(&I) -> Result<String> + Send + Sync + 'static,
{
    fn item_type(&self) -> TypeId {
        TypeId::of::<I>()
    }

    fn encode(&self, item: &(dyn Any + Send + Sync)) -> Result<String> {
        let item: &I = item.downcast_ref().ok_or(QueueError::AnyError)?;
        (self.f)(item)
    }
}

impl<I, F> IntoCustomEncoder<I> for F
where
    I: Send + Sync + 'static,
    F: Fn(&I) -> Result<String> + Send + Sync + 'static,
{
    fn into(self) -> Box<dyn CustomEncoder> {
        Box::new(FnEncoder {
            f: self,
            _i_pd: PhantomData,
        })
    }
}

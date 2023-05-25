use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::QueueError;

pub type EncoderRegistry<T> = Arc<HashMap<TypeId, Box<dyn CustomEncoder<T>>>>;

pub trait CustomEncoder<P: Send + Sync>: Send + Sync {
    fn item_type(&self) -> TypeId;
    fn encode(&self, item: &(dyn Any + Send + Sync)) -> Result<Box<P>, QueueError>;
}

pub trait IntoCustomEncoder<I: Send + Sync, O: Send + Sync> {
    fn into(self) -> Box<dyn CustomEncoder<O>>;
}

struct FnEncoder<
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
    > CustomEncoder<O> for FnEncoder<I, O, F>
{
    fn item_type(&self) -> TypeId {
        TypeId::of::<I>()
    }

    fn encode(&self, item: &(dyn Any + Send + Sync)) -> Result<Box<O>, QueueError> {
        let item: &I = item.downcast_ref().ok_or(QueueError::AnyError)?;
        (self.f)(item).map(Box::new)
    }
}

impl<
        I: 'static + Send + Sync,
        O: 'static + Send + Sync,
        F: 'static + Fn(&I) -> Result<O, QueueError> + Send + Sync,
    > IntoCustomEncoder<I, O> for F
{
    fn into(self) -> Box<dyn CustomEncoder<O>> {
        Box::new(FnEncoder {
            f: self,
            _i_pd: PhantomData,
            _o_pd: PhantomData,
        })
    }
}

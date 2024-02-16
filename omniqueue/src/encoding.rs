use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::QueueError;

pub type EncoderRegistry<T> = Arc<HashMap<TypeId, Box<dyn CustomEncoder<T>>>>;

pub trait CustomEncoder<P>: Send + Sync {
    fn item_type(&self) -> TypeId;
    fn encode(&self, item: &(dyn Any + Send + Sync)) -> Result<Box<P>, QueueError>;
}

pub trait IntoCustomEncoder<I, O> {
    fn into(self) -> Box<dyn CustomEncoder<O>>;
}

struct FnEncoder<I, O, F> {
    f: F,

    _i_pd: PhantomData<I>,
    _o_pd: PhantomData<O>,
}

impl<I, O, F> CustomEncoder<O> for FnEncoder<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> Result<O, QueueError> + Send + Sync + 'static,
{
    fn item_type(&self) -> TypeId {
        TypeId::of::<I>()
    }

    fn encode(&self, item: &(dyn Any + Send + Sync)) -> Result<Box<O>, QueueError> {
        let item: &I = item.downcast_ref().ok_or(QueueError::AnyError)?;
        (self.f)(item).map(Box::new)
    }
}

impl<I, O, F> IntoCustomEncoder<I, O> for F
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> Result<O, QueueError> + Send + Sync + 'static,
{
    fn into(self) -> Box<dyn CustomEncoder<O>> {
        Box::new(FnEncoder {
            f: self,
            _i_pd: PhantomData,
            _o_pd: PhantomData,
        })
    }
}

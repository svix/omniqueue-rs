macro_rules! impl_queue_consumer {
    (
        $ident:ident $( <$ty:ident: $tr:path> )?,
        $payload:ty
    ) => {
        #[deny(unconditional_recursion)] // method calls must defer to inherent methods
        impl<$($ty: $tr)?> crate::QueueConsumer for $ident<$($ty)?> {
            type Payload = $payload;

            fn receive(&mut self) -> impl std::future::Future<Output = Result<Delivery>> + Send {
                $ident::receive(self)
            }

            fn receive_all(
                &mut self,
                max_messages: usize,
                deadline: Duration,
            ) -> impl std::future::Future<Output = Result<Vec<Delivery>>> + Send {
                $ident::receive_all(self, max_messages, deadline)
            }
        }
    };
}

macro_rules! impl_queue_producer {
    (
        $ident:ident $( <$ty:ident: $tr:path> )?,
        $payload:ty
    ) => {
        #[deny(unconditional_recursion)] // method calls must defer to inherent methods
        impl<$($ty: $tr)?> crate::QueueProducer for $ident<$($ty)?> {
            type Payload = $payload;

            fn send_raw(
                &self,
                payload: &Self::Payload,
            ) -> impl std::future::Future<Output = Result<()>> + Send {
                $ident::send_raw(self, payload)
            }

            fn send_serde_json<P: serde::Serialize + Sync>(
                &self,
                payload: &P,
            ) -> impl std::future::Future<Output = Result<()>> + Send {
                $ident::send_serde_json(self, payload)
            }
        }
    };
}

macro_rules! impl_scheduled_queue_producer {
    (
        $ident:ident $( <$ty:ident: $tr:path> )?,
        $payload:ty
    ) => {
        #[deny(unconditional_recursion)] // method calls must defer to inherent methods
        impl<$($ty: $tr)?> crate::ScheduledQueueProducer for $ident<$($ty)?> {
            fn send_raw_scheduled(
                &self,
                payload: &Self::Payload,
                delay: Duration,
            ) -> impl std::future::Future<Output = Result<()>> + Send {
                $ident::send_raw_scheduled(self, payload, delay)
            }

            fn send_serde_json_scheduled<P: serde::Serialize + Sync>(
                &self,
                payload: &P,
                delay: Duration,
            ) -> impl std::future::Future<Output = Result<()>> + Send {
                $ident::send_serde_json_scheduled(self, payload, delay)
            }
        }
    };
}

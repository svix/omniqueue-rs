macro_rules! omni_delegate {
    ( receive ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn receive(&mut self) -> impl std::future::Future<Output = Result<Delivery>> + Send {
            Self::receive(self)
        }
    };
    ( receive_all ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn receive_all(
            &mut self,
            max_messages: usize,
            deadline: Duration,
        ) -> impl std::future::Future<Output = Result<Vec<Delivery>>> + Send {
            Self::receive_all(self, max_messages, deadline)
        }
    };
    ( send_raw ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn send_raw(
            &self,
            payload: &Self::Payload,
        ) -> impl std::future::Future<Output = Result<()>> + Send {
            Self::send_raw(self, payload)
        }
    };
    ( send_serde_json ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn send_serde_json<P: serde::Serialize + Sync>(
            &self,
            payload: &P,
        ) -> impl std::future::Future<Output = Result<()>> + Send {
            Self::send_serde_json(self, payload)
        }
    };
    ( send_raw_scheduled ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn send_raw_scheduled(
            &self,
            payload: &Self::Payload,
            delay: Duration,
        ) -> impl std::future::Future<Output = Result<()>> + Send {
            Self::send_raw_scheduled(self, payload, delay)
        }
    };
    ( send_serde_json_scheduled ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn send_serde_json_scheduled<P: serde::Serialize + Sync>(
            &self,
            payload: &P,
            delay: Duration,
        ) -> impl std::future::Future<Output = Result<()>> + Send {
            Self::send_serde_json_scheduled(self, payload, delay)
        }
    };
    ( redrive_dlq ) => {
        #[deny(unconditional_recursion)] // method call must defer to an inherent method
        fn redrive_dlq(
            &self,
        ) -> impl std::future::Future<Output = Result<()>> + Send {
            Self::redrive_dlq(self)
        }
    };

    ( $method1:ident, $($rest:ident),* $(,)? ) => {
        omni_delegate!($method1);
        omni_delegate!($($rest),*);
    }
}

use common::interface::ModuleTrait;
use std::sync::Arc;

mod test;

pub(crate) fn register_modules() -> Vec<Arc<dyn ModuleTrait>> {
    vec![
        test::wss_test::WssTest::default_arc(),
        test::moc_dev::MocDevModule::default_arc(),
    ]
}

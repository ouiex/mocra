use mocra::common::interface::ModuleTrait;
use std::sync::Arc;

mod test;

pub(crate) fn register_modules() -> Vec<Arc<dyn ModuleTrait>> {
    vec![test::moc_dev::MocDevModule::default_arc()]
}

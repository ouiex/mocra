use common::interface::ModuleTrait;
use std::sync::Arc;

mod test;

pub(crate) fn register_modules() -> Vec<Arc<dyn ModuleTrait>> {
    vec![
        test::add_chain_api::AddChainApiModule::default_arc(),
        test::moc_dev::MocDevModule::default_arc(),
        test::mock_dev::MockDevModule::default_arc(),
        test::portal_live_trend::PortalLiveTrend::default_arc(),
    ]
}

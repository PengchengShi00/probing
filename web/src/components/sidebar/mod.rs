//! Sidebar: logo, RL observability nav, tools, advanced submenu.
//! Uses [colors](crate::components::colors). Width/visibility in [state::sidebar](crate::state::sidebar).

use dioxus::prelude::*;
use dioxus_router::{Link, use_route};

use crate::app::Route;
use crate::components::colors::colors;
use crate::components::icon::Icon;
use crate::state::sidebar::{load_sidebar_state, save_sidebar_state, SIDEBAR_HIDDEN, SIDEBAR_WIDTH};

mod advanced;
mod nav_item;
mod profiling;
mod resize;

use advanced::AdvancedSidebarItem;
use nav_item::SidebarNavItem;
use profiling::ProfilingSidebarItem;
use resize::ResizeHandle;

fn sidebar_classes() -> (String, String, String, String, String, String) {
    (
        format!(
            "bg-gradient-to-b from-{} via-{} to-{} border-r border-{} h-screen flex flex-col flex-shrink-0 shadow-xl",
            colors::SIDEBAR_BG,
            colors::SIDEBAR_BG_VIA,
            colors::SIDEBAR_BG,
            colors::SIDEBAR_BORDER
        ),
        format!("px-4 py-3 border-b border-{}", colors::SIDEBAR_BORDER),
        format!("text-base font-semibold text-{}", colors::SIDEBAR_TEXT_PRIMARY),
        format!("px-4 py-3 border-t border-{}", colors::SIDEBAR_BORDER),
        format!(
            "flex items-center gap-2 text-xs text-{} hover:text-{} transition-colors",
            colors::SIDEBAR_TEXT_MUTED,
            colors::PRIMARY_TEXT_DARK
        ),
        format!(
            "absolute top-4 -right-3 w-6 h-6 bg-{} border border-slate-700 rounded-full shadow-lg flex items-center justify-center hover:bg-slate-600 z-30 transition-colors",
            colors::SIDEBAR_ACTIVE_BG
        ),
    )
}


#[component]
pub fn Sidebar() -> Element {
    let route = use_route::<Route>();
    let show_profiling_dropdown = use_signal(|| false);
    let show_advanced_dropdown = use_signal(|| false);

    use_effect(move || {
        load_sidebar_state();
    });

    let width = *SIDEBAR_WIDTH.read();
    let (aside, logo_border, brand, footer, footer_link, hide_btn) = sidebar_classes();
    let main_style = format!("width: {}px;", width);
    let section_label = format!(
        "px-3 pt-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-{}",
        colors::SIDEBAR_TEXT_MUTED
    );

    rsx! {
        div {
            class: "relative flex h-screen",
            style: "{main_style}",
            aside {
                class: "{aside}",
                style: "{main_style}",
                div {
                    class: "{logo_border}",
                    Link {
                        to: Route::RolloutPage {},
                        class: "flex items-center gap-2",
                        img { src: "{crate::utils::base_path::with_base(\"/assets/logo.svg\")}", alt: "Probing", class: "w-7 h-7 flex-shrink-0" }
                        span { class: "{brand}", "Probing" }
                    }
                }

                nav {
                    class: "flex-1 overflow-y-auto py-3",
                    div { class: "px-2 space-y-0.5",
                        div { class: "{section_label}", "RL" }
                        SidebarNavItem {
                            to: Route::RolloutPage {},
                            icon: &icondata::AiDeploymentUnitOutlined,
                            label: "Rollout",
                            is_active: matches!(route, Route::RolloutPage {} | Route::TracesPage {}),
                        }
                        SidebarNavItem {
                            to: Route::TrainPage {},
                            icon: &icondata::AiLineChartOutlined,
                            label: "Train",
                            is_active: route == Route::TrainPage {},
                        }
                        SidebarNavItem {
                            to: Route::SpansPage {},
                            icon: &icondata::AiApartmentOutlined,
                            label: "Spans",
                            is_active: route == Route::SpansPage {},
                        }
                        SidebarNavItem {
                            to: Route::ProcessTimelinePage {},
                            icon: &icondata::AiClockCircleOutlined,
                            label: "Process Timeline",
                            is_active: route == Route::ProcessTimelinePage {},
                        }
                        SidebarNavItem {
                            to: Route::PerfettoPage {},
                            icon: &icondata::AiThunderboltOutlined,
                            label: "Perfetto",
                            is_active: route == Route::PerfettoPage {},
                        }

                        div { class: "pt-3" }
                        div { class: "{section_label}", "Tools" }
                        ProfilingSidebarItem {
                            show_dropdown: show_profiling_dropdown,
                        }
                        SidebarNavItem {
                            to: Route::StackPage {},
                            icon: &icondata::AiThunderboltOutlined,
                            label: "Stacks",
                            is_active: route == Route::StackPage {},
                        }
                        SidebarNavItem {
                            to: Route::AnalyticsPage {},
                            icon: &icondata::AiAreaChartOutlined,
                            label: "Analytics",
                            is_active: route == Route::AnalyticsPage {},
                        }
                        SidebarNavItem {
                            to: Route::PulsingPage {},
                            icon: &icondata::AiApiOutlined,
                            label: "Pulsing",
                            is_active: route == Route::PulsingPage {},
                        }

                        div { class: "pt-3" }
                        AdvancedSidebarItem {
                            show_dropdown: show_advanced_dropdown,
                        }
                    }
                }

                div { class: "{footer}",
                    a {
                        href: "https://github.com/reiase/probing",
                        target: "_blank",
                        class: "{footer_link}",
                        Icon { icon: &icondata::AiGithubOutlined, class: "w-4 h-4" }
                        span { "GitHub" }
                    }
                }
            }

            button {
                class: "{hide_btn} focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2 focus:ring-offset-slate-900",
                title: "Hide Sidebar",
                aria_label: "Hide sidebar",
                onclick: move |_| {
                    *SIDEBAR_HIDDEN.write() = true;
                    save_sidebar_state();
                },
                Icon {
                    icon: &icondata::AiMenuFoldOutlined,
                    class: "w-4 h-4 text-slate-300"
                }
            }

            ResizeHandle {}
        }
    }
}

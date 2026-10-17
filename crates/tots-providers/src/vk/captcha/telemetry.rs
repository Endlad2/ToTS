//! Телеметрия виджета — массивы, которые VK сверяет с таймером.

use std::time::Duration;

use rand::Rng;

/// Строит форму телеметрии на основе прошедшего времени.
pub fn build_analytics(elapsed: Duration) -> Vec<(&'static str, String)> {
    // Виджет тикает раз в ~100мс; сколько тиков успело — столько сэмплов.
    let ticks = (elapsed.as_millis() / 100).min(600) as usize;
    let downlink_value = session_downlink();
    let downlink_samples = sample_downlink(downlink_value, ticks);

    vec![
        ("accelerometer", "[]".to_string()),
        ("gyroscope", "[]".to_string()),
        ("motion", "[]".to_string()),
        ("cursor", "[]".to_string()),
        ("taps", "[]".to_string()),
        ("connectionRtt", "[]".to_string()),
        ("connectionDownlink", downlink_samples),
    ]
}

fn sample_downlink(value: f64, ticks: usize) -> String {
    if ticks == 0 {
        return "[]".to_string();
    }
    let mut out = String::from("[");
    for i in 0..ticks {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format_downlink(value));
    }
    out.push(']');
    out
}

fn format_downlink(v: f64) -> String {
    // Печатаем через формат с минимальной точностью, как JSON.stringify.
    if (v - v.round()).abs() < 1e-9 {
        format!("{}", v.round() as i64)
    } else {
        let s = format!("{:.2}", v);
        // Обрезаем хвостовые нули.
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

/// Значение downlink у живого виджета постоянно в [8.0, 10.0].
fn session_downlink() -> f64 {
    let steps = rand::thread_rng().gen_range(160..=200);
    (steps * 5) as f64 / 100.0
}

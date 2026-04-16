"""
Runner unico per aggiornare dati, news e dataset finale di modeling.

Questo file serve quando vuoi lanciare in automatico la sequenza completa:
1. dataExtraction/rundataExtraction.py
2. newsExtraction/3.newsMaintenance.py
3. newsExtraction/4.textAnalysis.py
4. newsExtraction/5.weeklyNewsAggregation.py

Ogni step viene eseguito con lo stesso interprete Python attivo. Se uno script
fallisce, la pipeline si ferma subito e mostra quale passaggio ha generato
l'errore.
"""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


# ---------------------------------------------------------------------------
# PATH DI PROGETTO
# ---------------------------------------------------------------------------

ROOT_DIR = cfg.PROJECT_ROOT


# ---------------------------------------------------------------------------
# SCRIPT DA ESEGUIRE
# ---------------------------------------------------------------------------

# L'ordine e importante:
# - prima aggiorno la parte dataExtraction;
# - poi allineo le news al nuovo enterprises.csv;
# - poi ricalcolo la text analysis;
# - infine aggrego le news a livello settimanale e creo modeling.csv.
PIPELINE_STEPS = [
    {
        "name": "Data extraction completa",
        "path": cfg.DATA_EXTRACTION_RUNNER,
    },
    {
        "name": "Manutenzione news",
        "path": cfg.NEWS_MAINTENANCE_SCRIPT,
    },
    {
        "name": "Text analysis news",
        "path": cfg.NEWS_TEXT_ANALYSIS_SCRIPT,
    },
    {
        "name": "Aggregazione news settimanale",
        "path": cfg.NEWS_WEEKLY_AGGREGATION_SCRIPT,
    },
]


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO
# ---------------------------------------------------------------------------

def format_elapsed(seconds: float) -> str:
    # Stampo durate leggibili anche quando uno step dura parecchi minuti.
    minutes, remaining_seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h {minutes}m {remaining_seconds}s"
    if minutes:
        return f"{minutes}m {remaining_seconds}s"
    return f"{remaining_seconds}s"


def run_step(step: dict[str, Path | str]) -> float:
    # Esegue un singolo script e ritorna la durata dello step.
    step_name = str(step["name"])
    step_path = Path(step["path"])

    if not step_path.exists():
        raise FileNotFoundError(f"Script non trovato per '{step_name}': {step_path}")

    print("\n" + "=" * 90)
    print(f"Avvio step: {step_name}")
    print(f"Script: {step_path}")
    print("=" * 90)

    start_time = time.perf_counter()
    subprocess.run(
        [sys.executable, str(step_path)],
        cwd=ROOT_DIR,
        check=True,
    )
    elapsed_seconds = time.perf_counter() - start_time

    print(f"Step completato: {step_name} | durata: {format_elapsed(elapsed_seconds)}")
    return elapsed_seconds


# ---------------------------------------------------------------------------
# FLUSSO PRINCIPALE
# ---------------------------------------------------------------------------

def main() -> None:
    pipeline_start_time = time.perf_counter()
    completed_steps: list[dict[str, object]] = []

    print("Avvio pipeline completa data + news.")
    print(f"Python: {sys.executable}")
    print(f"Root progetto: {ROOT_DIR}")

    for step in PIPELINE_STEPS:
        elapsed_seconds = run_step(step)
        completed_steps.append(
            {
                "name": step["name"],
                "seconds": elapsed_seconds,
            }
        )

    total_elapsed_seconds = time.perf_counter() - pipeline_start_time

    print("\n" + "=" * 90)
    print("PIPELINE COMPLETA TERMINATA")
    print("=" * 90)
    for completed_step in completed_steps:
        print(
            f"- {completed_step['name']}: "
            f"{format_elapsed(float(completed_step['seconds']))}"
        )
    print(f"Durata totale: {format_elapsed(total_elapsed_seconds)}")


if __name__ == "__main__":
    main()

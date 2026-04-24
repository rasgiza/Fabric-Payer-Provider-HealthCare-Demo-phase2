"""Fix Cell 8 (index 7) to handle known Fabric result-parse bug."""
import json

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))
cell = nb["cells"][7]

new_source = [
    "# ============================================================================\n",
    "# CELL 6 \u2014 Generate synthetic data\n",
    "# ============================================================================\n",
    "# Uses notebookutils.notebook.run() \u2014 the native Fabric way to orchestrate\n",
    "# notebooks. More reliable than the Jobs REST API after updateDefinition.\n",
    "# ============================================================================\n",
    "\n",
    "if GENERATE_DATA:\n",
    '    print("Running NB_Generate_Sample_Data (generates ~10K patients, 100K encounters)...")\n',
    '    print("This takes 2-4 minutes.\\n")\n',
    "\n",
    "    try:\n",
    '        notebookutils.notebook.run("NB_Generate_Sample_Data", 1200, {"useRootDefaultLakehouse": True})\n',
    '        print("\\n\u2705 Data generation SUCCEEDED")\n',
    "    except Exception as e:\n",
    "        # Known Fabric bug: notebook succeeds but result-parsing throws\n",
    "        # NoSuchElementException on the snapshot MIME key. Treat as success.\n",
    '        if "mssparkutilsrun-result+json" in str(e) or "NoSuchElementException" in str(e):\n',
    '            print("\\n\u2705 Data generation SUCCEEDED (ignoring Fabric result-parse bug)")\n',
    "        else:\n",
    '            print(f"\\n\u274c Data generation FAILED: {e}")\n',
    '            print("Try running NB_Generate_Sample_Data manually from the workspace.")\n',
    "else:\n",
    '    print("Skipping data generation (GENERATE_DATA=False)")',
]

cell["source"] = new_source
json.dump(nb, open("Healthcare_Launcher.ipynb", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print("Done")

# Verify syntax
compile("".join(new_source), "cell6", "exec")
print("SYNTAX OK")

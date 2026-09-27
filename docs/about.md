# About this handbook

> Notes on how the handbook is written, kept current, and how to use it.

## Who writes it

I'm Sarang Ambekar, a data engineer. This handbook is the reference I wanted while learning and working: one place that connects the concepts, the tools and the production patterns, from a first query to a running pipeline, including the AI and LLM engineering that now sits next to it.

- GitHub: [sarangambekar1997](https://github.com/sarangambekar1997)
- Issues and corrections: [open an issue](https://github.com/sarangambekar1997/data-engineering-handbook/issues)

## How the guides work

Every guide follows the same shape, so you can find things without relearning the layout:

| Part | What it gives you |
|------|-------------------|
| Overview | The problem, the solution, and where it sits in a data platform |
| Basic → Intermediate → Advanced | Working code, in order of difficulty |
| Common Pitfalls | What goes wrong in production, and the fix |
| Cheat Sheet | Commands and syntax to copy |
| Interview Questions | Short answers with the trade-off |
| Further Reading | Vendor documentation |

The [labs](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs) and the [projects](projects/index.md) turn the guides into practice.

## Keeping it current

Tools change faster than books. Three things keep this one honest:

- **Every code block is parsed in CI**, and the labs run end to end, so examples don't silently rot.
- **Model IDs and prices are not hardcoded.** The AI guides link to the vendor pages and load prices from config. A scheduled check flags any retired model ID.
- **Every page shows when it was last updated**, taken from the Git history, and links are checked weekly.

If you find something out of date, please [tell me](https://github.com/sarangambekar1997/data-engineering-handbook/issues).

## License

Content and code are released under the [MIT License](https://github.com/sarangambekar1997/data-engineering-handbook/blob/main/LICENSE). See [Contributing](https://github.com/sarangambekar1997/data-engineering-handbook/blob/main/CONTRIBUTING.md) to help.

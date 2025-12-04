from openai import OpenAI
import config



CLIENT = OpenAI()

# rules for use_case 1 and 2 
MAIN_RULES = f"""OUTPUT SPEC (STRICT — FOLLOW EXACTLY):

    1) BULLETS
    - Write up to three concise bullets.
    - Each bullet must start with "- " (dash + space).
    - EVERY factual statement (numbers, percentages, dates, “up/down”, specific claims)
        MUST end with one or more citation markers.
    - Citation marker format (STRICT):
        [S# pPAGE "SHORT QUOTE"]
        Examples:
            [S1 p7 "traffic rose 3% year-on-year in FY25"]
            [S2 p3 "growth in comparable store sales during H1"]

    RULES FOR CITATION MARKERS:
        - S# is the source index shown in CANDIDATES (1-based).
        - PAGE is the page number from the [S# pN] prefix in the context.
        - SHORT QUOTE:
            * EXACT, verbatim text copied from the underlying context for that S and page.
            * Must be 6–12 consecutive words.
            * No paraphrasing, no substitutions, no reordering.
        - Place the marker IMMEDIATELY after the sentence or clause it supports.
        - A bullet may contain multiple markers if using multiple claims.

    CITATIONS(JSON)
    After the bullets, output EXACTLY this line:
        CITATIONS(JSON)
    On the next line output ONLY a valid JSON array, e.g.:
        [
        {{ "bullet": 1, "S": 1, "page": 7, "quote": "traffic rose 3% year-on-year in FY25" }},
        {{ "bullet": 1, "S": 2, "page": 3, "quote": "growth in comparable store sales during H1" }}
        ]

    JSON RULES:
        - One object per citation marker used in the bullets.
        - "bullet" is 1-based bullet index.
        - "S" matches the S# from the marker.
        - "page" is the same page number as in the marker.
        - "quote" exactly matches the SHORT QUOTE used inside that marker.
        - JSON must be valid: no comments, no trailing commas.

    Sources
    After the JSON array, output a “Sources” section:
        Sources
        - <Exact title from CANDIDATES> — p.N[, p.M ...] — "ONE REPRESENTATIVE QUOTE"

    RULES:
        - The first line must be exactly: Sources
        - Then one bullet line per DISTINCT cited source.
        - Titles MUST match exactly what appears in CANDIDATES.
        - List ALL cited pages for that source, sorted ascending (e.g. "p.3, p.4, p.9").
        - The trailing quoted text must be ONE of the SHORT QUOTEs you used for that source.
        - All text must be copied exactly from context—no paraphrasing.
        - You can only use page 1 as a last resort [Sx p1]. Page 1 is an overview, and we want to find the explained section. 

    GENERAL RULES (CRITICAL):
    - The most important Criteria for choosing sources is THE MOST RECENT, RELEVANT SOURCE.
    - YOU MUST, for every single dotpoint have the date of the given report Listed in month-yyyy (Jan-2025) at the start of the dotpoint. 
    - When answering with metrics, you MUST NOT quote multiple of the same/ similar metrics from multiple reports. You must only choose the most upto date metric, and quote in brackets the date when it was given. 
    - You MUST NOT use ANY information not found in the provided context.
    - If the context does not include sufficient information, write fewer bullets or none.
    - Every factual assertion must be grounded in a verbatim quote.
    - If you cannot find a valid 6–12 word quote, you may NOT make the claim.
    - You may skip bullets entirely if the source text is too thin.
    - Precision over breadth: fewer correct bullets > more speculative content.
    - Use Australian-English spelling and syntax pelase. 
"""

# rules for use_case_3
EXPANSION_RULES = f"""
    OUTPUT SPEC (STRICT — FOLLOW EXACTLY):

    1) BULLETS
    - Produce ONE expanded bullet that provides deeper, more specific detail than the original bullet.
    - Write up to three concise bullets.
    - Each bullet must start with "- " (dash + space).
    - EVERY factual statement (numbers, percentages, dates, “up/down”, specific claims)
        MUST end with one or more citation markers.
    - Citation marker format (STRICT):
        [S# pPAGE "SHORT QUOTE"]
        Examples:
            [S1 p7 "traffic rose 3% year-on-year in FY25"]
            [S2 p3 "growth in comparable store sales during H1"]


    RULES FOR CITATION MARKERS:
        - S# is the source index shown in CANDIDATES (1-based).
        - PAGE is the page number from the [S# pN] prefix in the context.
        - SHORT QUOTE:
            * EXACT, verbatim text copied from the underlying context for that S and page.
            * Must be 6–12 consecutive words.
            * No paraphrasing, no substitutions, no reordering.
        - Place the marker IMMEDIATELY after the sentence or clause it supports.
        - A bullet may contain multiple markers if using multiple claims.

    CITATIONS(JSON)
    After the bullets, output EXACTLY this line:
        CITATIONS(JSON)
    On the next line output ONLY a valid JSON array, e.g.:
        [
        {{ "bullet": 1, "S": 1, "page": 7, "quote": "traffic rose 3% year-on-year in FY25" }},
        {{ "bullet": 1, "S": 2, "page": 3, "quote": "growth in comparable store sales during H1" }}
        ]

    JSON RULES:
        - One object per citation marker used in the bullets.
        - "bullet" is 1-based bullet index.
        - "S" matches the S# from the marker.
        - "page" is the same page number as in the marker.
        - "quote" exactly matches the SHORT QUOTE used inside that marker.
        - JSON must be valid: no comments, no trailing commas.

    Sources
    After the JSON array, output a “Sources” section:
        Sources
        - <Exact title from CANDIDATES> — p.N[, p.M ...] — "ONE REPRESENTATIVE QUOTE"

    GENERAL RULES:
    - DO NOT introduce new facts not present in the document.
    - DO NOT infer or guess beyond the source text.
    - DO NOT mention or summarise the original bullet.
    - DO NOT output sub-bullets.
    - DO NOT output any text outside the single expanded bullet.
    - Use Australian-English spelling.

    THE ONLY OUTPUT MUST BE:
    - A single expanded bullet, starting with "- ".
"""


REFORMULATE_RULES = F"""
    You are an expert Australian retail analyst.
    You rewrite user questions into sharper, more directed analytical questions
    using ONLY the high-level overview text provided (page 1 of reports).
    WE MUST PRIORITISE MOST RECENT REPORTS and INFORMATION FIRST. 

    RULES:
    - Do NOT introduce any new numeric claims.
    - Do NOT invent facts.
    - Your rewrite should reflect the themes/topics the reports emphasise.
    - Make the question more specific so the downstream LLM looks for
      detailed quantified metrics in later pages.
    - Output ONLY the rewritten question with no explanation.
"""




# Helper Functions 


def create_system_prompt(use_case, expand_func_last_main_query=None):
    if use_case != "use_case_3":
        base_persona = (
            "Persona: CEO-brief writer for Australian retail. Use ONLY the provided internal context.\n"
            "Priorities: newest first; crisp, quantified forward-looking bullets. No investment advice."
            if use_case != "use_case_2" else
            "Persona: Sector/macro brief writer. Use ONLY the provided internal context.\n"
            "Priorities: newest first; sector-level quantified bullets. No investment advice."
        )
        rules = MAIN_RULES
    elif use_case == "use_case_3":
        base_persona = (
            "Persona: Detailed report expansion analyst for Australian retail.\n"
            "Your task is to expand upon the user's bullet by providing deeper, richer, more specific detail.\n"
            "Use ONLY the provided document as your factual basis.\n"
            "Focus on: operational detail, drivers, category-level insights, segment commentary, time context,\n"
            "and any additional explanations that appear in the document.\n"
            "NEVER invent information. NEVER restate the original bullet. NEVER generalise.\n"
            "Your output must be ONE newly expanded bullet using strict citation markers.\n"
            f"You must expand the report with specific relation to the given question. {expand_func_last_main_query}"

        )
        rules = EXPANSION_RULES

    else:
        print(f"opanai_manager:create_persona:ERROR: invalid usecase: {use_case}") 
        base_persona = None 
        rules = None



    return base_persona, rules








# =========================================
# MAIN FUNCTIONS 
# =========================================



def reformulate_query(user_query: str, page1_blocks: List[str], candidates_block) -> str:
    """
    Uses page-1 overview text to rewrite the user's question into a more
    specific, context-directed analytical question.
    """

    if not page1_blocks:
        return user_query

    # format the blocks 
    joined_page1 = "\n\n".join(page1_blocks)

    # define user input 
    user = f"""
    ORIGINAL QUESTION:
    {user_query}

    OVERVIEW CONTEXT (PAGE 1):
    {joined_page1}

    DOCUMENT REFERENCES FOR THE PAGES: 
    {candidates_block}

    Rewrite the question now:
    """

    # send to llm 
    r = CLIENT.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[
            {"role": "system", "content": REFORMULATE_RULES},
            {"role": "user", "content": user}
        ],
        temperature=0.2,
    )

    return (r.choices[0].message.content or "").strip()

def main_answer(query, candidates_block, sources_text, use_case, expand_func_last_main_query=None):
    # define persona, based on use_case
    persona, rules = create_system_prompt(use_case, expand_func_last_main_query=expand_func_last_main_query)

    # format system and user prompt 
    system = persona + "\n\n" + MAIN_RULES + "\nCANDIDATES:\n" + candidates_block
    user = (
        f"User query: {query}\n\n"
        f"Context snippets (each prefixed with [S# pN]):\n{sources_text}\n\n"
        "Write the bullets, then the CITATIONS(JSON) array, then the final 'Sources' section now. "
        "Do not add anything else."
    )

    # send to llm 
    r = CLIENT.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        temperature=0.2,
    )
    out = (r.choices[0].message.content or "").strip()
    
    print(f"openai_manager:main_answer:DEBUG: llm_response: {out}")
    return out

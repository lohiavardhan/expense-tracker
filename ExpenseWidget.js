// Expense Tracker Widget for Scriptable
const API_BASE = "http://98:82:115:127:8051"

async function fetchWidgetData() {
  const url = `${API_BASE}/widget`
  const req = new Request(url)
  req.timeoutInterval = 10
  return await req.loadJSON()
}

function formatSGD(amount) {
  return "S$" + amount.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",")
}

function formatDate(isoStr) {
  if (!isoStr) return ""
  return isoStr.slice(0, 10)
}

async function createWidget(data) {
  const widget = new ListWidget()

  // Background gradient
  const grad = new LinearGradient()
  grad.locations = [0, 1]
  grad.colors = [new Color("#1a1a2e"), new Color("#16213e")]
  widget.backgroundGradient = grad
  widget.setPadding(14, 16, 14, 16)

  // Header row
  const headerStack = widget.addStack()
  headerStack.layoutHorizontally()
  headerStack.centerAlignContent()

  const titleText = headerStack.addText("Expenses")
  titleText.font = Font.boldSystemFont(15)
  titleText.textColor = Color.white()

  headerStack.addSpacer()

  const updatedAt = data.generated_at ? data.generated_at.slice(0, 16).replace("T", " ") : ""
  const updText = headerStack.addText(updatedAt)
  updText.font = Font.systemFont(9)
  updText.textColor = new Color("#ffffff", 0.4)

  widget.addSpacer(8)

  // Daily / Monthly spend row
  const spendRow = widget.addStack()
  spendRow.layoutHorizontally()
  spendRow.spacing = 10

  function addMetricBox(stack, label, value) {
    const box = stack.addStack()
    box.layoutVertically()
    box.backgroundColor = new Color("#ffffff", 0.07)
    box.cornerRadius = 10
    box.setPadding(8, 10, 8, 10)

    const lbl = box.addText(label)
    lbl.font = Font.systemFont(9)
    lbl.textColor = new Color("#ffffff", 0.5)

    box.addSpacer(2)

    const val = box.addText(value)
    val.font = Font.boldSystemFont(14)
    val.textColor = new Color("#4fc3f7")
    val.minimumScaleFactor = 0.7
    val.lineLimit = 1

    return box
  }

  addMetricBox(spendRow, "TODAY", formatSGD(data.daily_spend))
  spendRow.addSpacer()
  addMetricBox(spendRow, "THIS CYCLE", formatSGD(data.monthly_spend))

  widget.addSpacer(10)

  // Divider
  const divider = widget.addStack()
  divider.backgroundColor = new Color("#ffffff", 0.12)
  divider.size = new Size(0, 1)
  divider.cornerRadius = 1

  widget.addSpacer(8)

  // Top merchants header
  const merchantsHeader = widget.addText("Top Merchants")
  merchantsHeader.font = Font.semiboldSystemFont(10)
  merchantsHeader.textColor = new Color("#ffffff", 0.5)

  widget.addSpacer(5)

  // Top 5 merchants list
  const merchants = data.top_5_merchants || []
  for (let i = 0; i < Math.min(merchants.length, 5); i++) {
    const m = merchants[i]

    const row = widget.addStack()
    row.layoutHorizontally()
    row.centerAlignContent()
    row.spacing = 4

    // Rank badge
    const rank = row.addText(`${i + 1}`)
    rank.font = Font.systemFont(9)
    rank.textColor = new Color("#ffffff", 0.3)

    const name = row.addText(m.merchant || "Unknown")
    name.font = Font.systemFont(11)
    name.textColor = Color.white()
    name.lineLimit = 1
    name.minimumScaleFactor = 0.75

    row.addSpacer()

    const amt = row.addText(formatSGD(m.total))
    amt.font = Font.mediumSystemFont(11)
    amt.textColor = new Color("#81c784")

    if (i < Math.min(merchants.length, 5) - 1) {
      widget.addSpacer(4)
    }
  }

  if (merchants.length === 0) {
    const empty = widget.addText("No merchant data")
    empty.font = Font.systemFont(11)
    empty.textColor = new Color("#ffffff", 0.3)
  }

  widget.addSpacer()

  // Cycle label at bottom
  if (data.cycle_start && data.cycle_end) {
    const cycleLabel = widget.addText(`Cycle: ${formatDate(data.cycle_start)} – ${formatDate(data.cycle_end)}`)
    cycleLabel.font = Font.systemFont(8)
    cycleLabel.textColor = new Color("#ffffff", 0.25)
  }

  return widget
}

async function createErrorWidget(message) {
  const widget = new ListWidget()
  widget.backgroundColor = new Color("#1a1a2e")
  widget.setPadding(16, 16, 16, 16)

  const title = widget.addText("Expense Tracker")
  title.font = Font.boldSystemFont(13)
  title.textColor = Color.white()

  widget.addSpacer(8)

  const err = widget.addText(message)
  err.font = Font.systemFont(11)
  err.textColor = new Color("#ef5350")
  err.lineLimit = 3

  return widget
}

// --- Main ---
let widget
try {
  const data = await fetchWidgetData()
  widget = await createWidget(data)
} catch (e) {
  widget = await createErrorWidget(`Could not load data:\n${e.message}`)
}

if (config.runsInWidget) {
  Script.setWidget(widget)
} else {
  await widget.presentMedium()
}

Script.complete()

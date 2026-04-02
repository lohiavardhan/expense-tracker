// Expense Tracker Widget for Scriptable
const API_BASE = "http://98.82.115.127:8051"

async function fetchWidgetData() {
  const url = `${API_BASE}/widget`
  const req = new Request(url)
  req.timeoutInterval = 10
  return await req.loadJSON()
}

function formatSGD(amount) {
  return "S$" + amount.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",")
}

async function createWidget(data) {
  const widget = new ListWidget()
  widget.backgroundColor = new Color("#000000")
  widget.setPadding(16, 16, 16, 16)

  const todayLabel = widget.addText("TODAY")
  todayLabel.font = Font.systemFont(11)
  todayLabel.textColor = new Color("#ffffff", 0.5)

  const todayValue = widget.addText(formatSGD(data.daily_spend))
  todayValue.font = Font.boldSystemFont(22)
  todayValue.textColor = Color.white()

  widget.addSpacer(12)

  const cycleLabel = widget.addText("THIS CYCLE")
  cycleLabel.font = Font.systemFont(11)
  cycleLabel.textColor = new Color("#ffffff", 0.5)

  const cycleValue = widget.addText(formatSGD(data.monthly_spend))
  cycleValue.font = Font.boldSystemFont(22)
  cycleValue.textColor = Color.white()

  widget.addSpacer()

  return widget
}

async function createErrorWidget(message) {
  const widget = new ListWidget()
  widget.backgroundColor = new Color("#000000")
  widget.setPadding(16, 16, 16, 16)

  const err = widget.addText(message)
  err.font = Font.systemFont(11)
  err.textColor = new Color("#ef5350")
  err.lineLimit = 3

  return widget
}

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
  await widget.presentSmall()
}

Script.complete()

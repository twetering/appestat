// Local OCR using Apple's Vision framework (macOS only, no network).
// Usage: swift scripts/vision_ocr.swift <image>   (or a compiled binary)
// Prints JSON: {"captured_at": "YYYY-MM-DD HH:MM:SS" | null, "rotation": "up|right|left|down",
//               "lines": [{"text", "conf", "x", "y", "w", "h"}, ...]}
// Coordinates are normalized (0-1) with origin at the TOP-left of the upright receipt.
import CoreImage
import Foundation
import ImageIO
import Vision

func fail(_ msg: String) -> Never {
    FileHandle.standardError.write((msg + "\n").data(using: .utf8)!)
    exit(1)
}

guard CommandLine.arguments.count > 1 else { fail("usage: vision_ocr <image>") }

let url = URL(fileURLWithPath: CommandLine.arguments[1])
guard let source = CGImageSourceCreateWithURL(url as CFURL, nil) else { fail("cannot load image") }
let props = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any] ?? [:]

// Local wall-clock time the photo was taken; fallback date when the receipt's own date is cut off.
var capturedAt: Any = NSNull()
if let exif = props[kCGImagePropertyExifDictionary] as? [CFString: Any],
   let original = exif[kCGImagePropertyExifDateTimeOriginal] as? String {
    // EXIF format is "YYYY:MM:DD HH:MM:SS"
    let parts = original.split(separator: " ", maxSplits: 1)
    if parts.count == 2 {
        capturedAt = parts[0].replacingOccurrences(of: ":", with: "-") + " " + parts[1]
    }
}

// Apply the EXIF rotation so "up" means upright as the phone saw it.
guard let upright = CIImage(contentsOf: url, options: [.applyOrientationProperty: true]) else {
    fail("cannot decode image")
}
let pixelW = upright.extent.width, pixelH = upright.extent.height

struct Line {
    let text: String
    let conf: Float
    let box: CGRect  // normalized, Vision's bottom-left origin
}

func recognize(_ orientation: CGImagePropertyOrientation) -> [Line] {
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.recognitionLanguages = ["nl-NL", "en-US"]
    request.usesLanguageCorrection = false  // receipts are abbreviations, not prose
    let handler = VNImageRequestHandler(ciImage: upright, orientation: orientation, options: [:])
    do { try handler.perform([request]) } catch { return [] }
    return (request.results ?? []).compactMap { obs in
        guard let cand = obs.topCandidates(1).first else { return nil }
        return Line(text: cand.string, conf: cand.confidence, box: obs.boundingBox)
    }
}

// Receipts are often photographed sideways. Text read in the wrong orientation still
// comes back, but as tall narrow boxes; and upside-down shows the store name below the total.
func score(_ lines: [Line], _ orientation: CGImagePropertyOrientation) -> Double {
    let sideways = orientation == .right || orientation == .left
    let (w, h) = sideways ? (pixelH, pixelW) : (pixelW, pixelH)
    var s = 0.0
    var plusY: CGFloat? = nil, totaalY: CGFloat? = nil
    for l in lines where l.text.count >= 3 {
        if l.box.width * w > l.box.height * h { s += Double(l.conf) * Double(l.text.count) }
        let lower = l.text.lowercased()
        if lower.contains("plus") && !lower.contains("punt") { plusY = max(plusY ?? 0, l.box.midY) }
        if lower.hasSuffix("otaal") && !lower.contains("sub") { totaalY = l.box.midY }
    }
    // Vision's y grows upward, so the store header must sit above the total.
    if let p = plusY, let t = totaalY, p < t { s *= 0.5 }
    return s
}

var best: (CGImagePropertyOrientation, [Line], Double) = (.up, recognize(.up), 0)
best.2 = score(best.1, .up)
let horizontalShare = best.2 / max(1, best.1.filter { $0.text.count >= 3 }
    .reduce(0.0) { $0 + Double($1.conf) * Double($1.text.count) })
if horizontalShare < 0.8 {
    for o in [CGImagePropertyOrientation.right, .left, .down] {
        let lines = recognize(o)
        let s = score(lines, o)
        if s > best.2 { best = (o, lines, s) }
    }
}

let names: [CGImagePropertyOrientation: String] = [.up: "up", .right: "right", .left: "left", .down: "down"]
let lines: [[String: Any]] = best.1.map { l in
    ["text": l.text, "conf": l.conf,
     "x": l.box.minX, "y": 1 - l.box.maxY, "w": l.box.width, "h": l.box.height]
}
let out: [String: Any] = ["captured_at": capturedAt, "rotation": names[best.0] ?? "up", "lines": lines]
let data = try JSONSerialization.data(withJSONObject: out, options: [])
print(String(data: data, encoding: .utf8)!)

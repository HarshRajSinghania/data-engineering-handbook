// Adds a "Print this cheat sheet" button to the Cheat Sheet section of a guide.
// Printing hides everything on the page except the guide's title and that section (see extra.css).
document$.subscribe(function () {
  var heading = document.getElementById("cheat-sheet");
  if (!heading || heading.tagName !== "H2" || heading.querySelector(".print-cheatsheet")) return;

  var button = document.createElement("button");
  button.type = "button";
  button.className = "print-cheatsheet";
  button.textContent = "Print this cheat sheet";
  button.addEventListener("click", function () {
    var article = heading.closest("article");
    var marked = [];
    var mark = function (node) { node.classList.add("cheatsheet-section"); marked.push(node); };

    var title = article.querySelector("h1");
    if (title) mark(title);
    for (var node = heading; node && !(node !== heading && node.tagName === "H2"); node = node.nextElementSibling) {
      mark(node);
    }
    document.body.classList.add("print-cheatsheet-only");

    var cleanup = function () {
      document.body.classList.remove("print-cheatsheet-only");
      marked.forEach(function (n) { n.classList.remove("cheatsheet-section"); });
    };
    window.addEventListener("afterprint", cleanup, { once: true });
    window.print();
  });
  heading.appendChild(button);
});

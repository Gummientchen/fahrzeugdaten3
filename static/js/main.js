// SCRIPT BLOCK 1: GLOBAL DEFINITIONS FOR HISTORY
const MAX_HISTORY_ITEMS = 10;
const HISTORY_COOKIE_NAME = "carSearchHistory";

function setCookie(name, value, days) {
  let expires = "";
  if (days) {
    const date = new Date();
    date.setTime(date.getTime() + days * 24 * 60 * 60 * 1000);
    expires = "; expires=" + date.toUTCString();
  }
  document.cookie =
    name + "=" + (value || "") + expires + "; path=/; SameSite=Lax";
}

function getCookie(name) {
  const nameEQ = name + "=";
  const ca = document.cookie.split(";");
  for (let i = 0; i < ca.length; i++) {
    let c = ca[i];
    while (c.charAt(0) === " ") c = c.substring(1, c.length);
    if (c.indexOf(nameEQ) === 0) return c.substring(nameEQ.length, c.length);
  }
  return null;
}

window.addSearchToHistory = function (tgCode, displayName) {
  if (!tgCode || !displayName) return;
  let history = [];
  const savedHistory = getCookie(HISTORY_COOKIE_NAME); // Uses global getCookie
  if (savedHistory) {
    try {
      history = JSON.parse(savedHistory);
    } catch (e) {
      console.error(
        "Error parsing search history cookie in addSearchToHistory",
        e
      );
      history = [];
    }
  }
  history = history.filter((item) => item.tgCode !== tgCode);
  history.unshift({ tgCode: tgCode, display: displayName });
  if (history.length > MAX_HISTORY_ITEMS) {
    history = history.slice(0, MAX_HISTORY_ITEMS);
  }
  setCookie(HISTORY_COOKIE_NAME, JSON.stringify(history), 180); // Uses global setCookie
};

// SCRIPT BLOCK 2: DOM MANIPULATION AND EVENT LISTENERS
function setupAutocomplete(
  inputId,
  suggestionsId,
  endpointUrl,
  isTypField = false
) {
  const inputField = document.getElementById(inputId);
  const suggestionsContainer = document.getElementById(suggestionsId);
  let debounceTimer;

  inputField.addEventListener("input", function () {
    clearTimeout(debounceTimer);
    const term = this.value;

    debounceTimer = setTimeout(async () => {
      suggestionsContainer.innerHTML = ""; // Clear previous suggestions
      suggestionsContainer.style.display = "none";

      if (term.length < 1) {
        // Minimum characters to trigger autocomplete
        return;
      }

      let currentEndpointUrl = `${endpointUrl}?term=${encodeURIComponent(
        term
      )}`;
      if (isTypField) {
        const markeValue = document.getElementById("marke_input").value;
        if (markeValue && markeValue.trim()) {
          // Ensure markeValue is not empty and not just whitespace
          currentEndpointUrl += `&marke=${encodeURIComponent(
            markeValue.trim()
          )}`;
        }
      }

      try {
        const response = await fetch(currentEndpointUrl);
        if (!response.ok) {
          console.error(
            "Autocomplete fetch error:",
            response.status,
            response.statusText
          );
          return;
        }
        const suggestions = await response.json();

        if (suggestions.length > 0) {
          suggestionsContainer.style.display = "block";
          suggestions.forEach((suggestion) => {
            const div = document.createElement("div");
            div.textContent = suggestion;
            div.addEventListener("click", function () {
              inputField.value = suggestion;
              suggestionsContainer.innerHTML = "";
              suggestionsContainer.style.display = "none";
            });
            suggestionsContainer.appendChild(div);
          });
        }
      } catch (error) {
        console.error("Autocomplete request failed:", error);
      }
    }, 250); // 250ms debounce delay
  });

  document.addEventListener("click", function (event) {
    if (
      !inputField.contains(event.target) &&
      !suggestionsContainer.contains(event.target)
    ) {
      suggestionsContainer.innerHTML = "";
      suggestionsContainer.style.display = "none";
    }
  });
}

document.addEventListener("DOMContentLoaded", function () {
  setupAutocomplete("marke_input", "marke_suggestions", "/autocomplete/marken");
  setupAutocomplete(
    "typ_input",
    "typ_suggestions",
    "/autocomplete/typen",
    true
  ); // Pass true for isTypField

  function displaySearchHistory() {
    const historyList = document.getElementById("search_history_list");
    historyList.innerHTML = ""; // Clear previous items
    const savedHistory = getCookie(HISTORY_COOKIE_NAME);
    let historyItems = []; // Initialize as empty array

    if (savedHistory) {
      try {
        historyItems = JSON.parse(savedHistory);
      } catch (e) {
        console.error("Error parsing search history cookie for display", e);
        historyItems = []; // Reset to empty on error if parsing fails
      }
    }

    if (historyItems && historyItems.length > 0) {
      historyItems.forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item.display;
        li.dataset.tgcode = item.tgCode;
        li.addEventListener("click", function () {
          document.getElementById("tg_code_input").value = this.dataset.tgcode;
          document.getElementById("marke_input").value = "";
          document.getElementById("typ_input").value = "";
          document.querySelector(".search-form form").submit();
        });
        historyList.appendChild(li);
      });
    } else {
      const li = document.createElement("li");
      li.textContent = "No history yet.";
      li.style.cursor = "default";
      historyList.appendChild(li);
    }
  }

  // Accordion Logic
  const accHeaders = document.querySelectorAll(".accordion-header");
  accHeaders.forEach((header) => {
    header.addEventListener("click", function () {
      this.classList.toggle("active");
      const panel = this.nextElementSibling;
      if (panel.style.display === "block") {
        panel.style.display = "none";
      } else {
        panel.style.display = "block";
      }
      // Save open accordion state to cookie
      const openAccordions = [];
      accHeaders.forEach((h) => {
        if (h.classList.contains("active")) {
          openAccordions.push(h.textContent.trim());
        }
      });
      setCookie("openAccordions", JSON.stringify(openAccordions), 7); // Uses global setCookie
    });
  });

  // Restore accordion state from cookie or open the first one
  if (accHeaders.length > 0) {
    // The presence of accordion headers implies we are on a page where their state should be managed.
    // The check for "Car Details" h2 text was problematic after translation.
    const savedOpenAccordions = getCookie("openAccordions"); // Uses your existing cookie name
    if (savedOpenAccordions) {
      try {
        const openLabels = JSON.parse(savedOpenAccordions);
        if (Array.isArray(openLabels)) { // Ensure it's an array
          accHeaders.forEach((header) => {
            if (openLabels.includes(header.textContent.trim())) {
              header.classList.add("active");
              // Ensure the panel exists and is an accordion panel
              if (header.nextElementSibling && header.nextElementSibling.classList.contains('accordion-panel')) {
                header.nextElementSibling.style.display = "block";
              }
            }
          });
        }
      } catch (e) {
        console.error("Error parsing accordion cookie 'openAccordions':", e);
        // Optionally, clear the bad cookie: setCookie("openAccordions", "", -1);
      }
    } else {
      // If no cookie, or cookie was invalid, open the first accordion by default
      if (accHeaders.length > 0) { // Check again in case accHeaders was empty initially
        const firstHeader = accHeaders[0];
        firstHeader.classList.add("active");
        if (firstHeader.nextElementSibling && firstHeader.nextElementSibling.classList.contains('accordion-panel')) {
              header.nextElementSibling.style.display = "block";
            }
      }
    }
  }

  const historyBtn = document.getElementById("search_history_btn");
  const historyContainer = document.getElementById("search_history_container");

  if (historyBtn && historyContainer) {
    historyBtn.addEventListener("click", function (event) {
      event.stopPropagation();
      const isVisible = historyContainer.style.display === "block";
      if (isVisible) {
        historyContainer.style.display = "none";
      } else {
        displaySearchHistory();
        historyContainer.style.display = "block";
      }
    });

    document.addEventListener("click", function (event) {
      if (
        historyContainer.style.display === "block" &&
        !historyContainer.contains(event.target) &&
        !historyBtn.contains(event.target)
      ) {
        historyContainer.style.display = "none";
      }
    });
  }

  // Handle clicks on TG-Codes in multiple results
  const multipleResultsList = document.querySelector(".multiple-results-list");
  if (multipleResultsList) {
    multipleResultsList.addEventListener("click", function (event) {
      let targetElement = event.target;
      while (
        targetElement != null &&
        !targetElement.classList.contains("clickable-tg-code")
      ) {
        targetElement = targetElement.parentElement;
      }

      if (
        targetElement &&
        targetElement.classList.contains("clickable-tg-code")
      ) {
        const tgCode = targetElement.dataset.tgcode;
        if (tgCode) {
          document.getElementById("tg_code_input").value = tgCode;
          document.getElementById("marke_input").value = "";
          document.getElementById("typ_input").value = "";
          document.querySelector(".search-form form").submit();
        }
      }
    });
  }
});

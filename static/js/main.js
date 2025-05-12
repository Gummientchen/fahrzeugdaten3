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

  if (!inputField || !suggestionsContainer) {
    // console.error("Autocomplete setup failed: Input or suggestions container not found for", inputId);
    return;
  }

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
        const markeValueElement = document.getElementById("marke_input");
        if (markeValueElement) {
            const markeValue = markeValueElement.value;
            if (markeValue && markeValue.trim()) {
              currentEndpointUrl += `&marke=${encodeURIComponent(
                markeValue.trim()
              )}`;
            }
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
  // Dark Mode Logic (Automatic based on system preference)
  const body = document.body;
  const prefersDarkScheme = window.matchMedia("(prefers-color-scheme: dark)");

  function applySystemThemePreference(event) {
    // event can be the initial load (event.matches) or a change event
    if (event.matches) {
      body.classList.add("dark-mode");
    } else {
      body.classList.remove("dark-mode");
    }
  }

  // Apply the theme based on initial system preference
  applySystemThemePreference(prefersDarkScheme);

  // Listen for changes in system preference
  try {
    prefersDarkScheme.addEventListener("change", applySystemThemePreference);
  } catch (e) { // Fallback for older browsers
    prefersDarkScheme.addListener(applySystemThemePreference);
  }
  // End Dark Mode Logic

  setupAutocomplete("marke_input", "marke_suggestions", "/autocomplete/marken");
  setupAutocomplete(
    "typ_input",
    "typ_suggestions",
    "/autocomplete/typen",
    true
  ); // Pass true for isTypField

  function displaySearchHistory() {
    const historyList = document.getElementById("search_history_list");
    if (!historyList) return; // Guard clause

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
          const tgCodeInputElement = document.getElementById("tg_code_input");
          const markeInputElement = document.getElementById("marke_input");
          const typInputElement = document.getElementById("typ_input");
          const searchFormElement = document.querySelector(".search-form form");

          if (tgCodeInputElement) tgCodeInputElement.value = this.dataset.tgcode;
          if (markeInputElement) markeInputElement.value = "";
          if (typInputElement) typInputElement.value = "";
          if (searchFormElement) searchFormElement.submit();
        });
        historyList.appendChild(li);
      });
    } else {
      const li = document.createElement("li");
      li.textContent = "Noch kein Suchverlauf."; // Translated
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
      // Ensure the panel exists and is an accordion panel
      if (panel && panel.classList.contains('accordion-panel')) {
        if (panel.style.display === "block") {
          panel.style.display = "none";
        } else {
          panel.style.display = "block";
        }
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
    const savedOpenAccordions = getCookie("openAccordions");
    if (savedOpenAccordions) {
      try {
        const openLabels = JSON.parse(savedOpenAccordions);
        if (Array.isArray(openLabels)) {
          accHeaders.forEach((header) => {
            if (openLabels.includes(header.textContent.trim())) {
              header.classList.add("active");
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
      if (accHeaders.length > 0) {
        const firstHeader = accHeaders[0];
        firstHeader.classList.add("active");
        if (firstHeader.nextElementSibling && firstHeader.nextElementSibling.classList.contains('accordion-panel')) {
          firstHeader.nextElementSibling.style.display = "block";
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
        displaySearchHistory(); // Populate history before showing
        historyContainer.style.display = "block";
      }
    });

    // Close history dropdown if clicked outside
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
      // Traverse up to find the clickable-tg-code element or the li parent
      while (
        targetElement != null &&
        !targetElement.classList.contains("clickable-tg-code")
      ) {
        if (targetElement.tagName === 'LI') break; // Stop if we reach the LI without finding the div
        targetElement = targetElement.parentElement;
      }

      if (
        targetElement &&
        targetElement.classList.contains("clickable-tg-code")
      ) {
        const tgCode = targetElement.dataset.tgcode;
        if (tgCode) {
          const tgCodeInputElement = document.getElementById("tg_code_input");
          const markeInputElement = document.getElementById("marke_input");
          const typInputElement = document.getElementById("typ_input");
          const searchFormElement = document.querySelector(".search-form form");

          if (tgCodeInputElement) tgCodeInputElement.value = tgCode;
          if (markeInputElement) markeInputElement.value = "";
          if (typInputElement) typInputElement.value = "";
          if (searchFormElement) searchFormElement.submit();
        }
      }
    });
  }
});

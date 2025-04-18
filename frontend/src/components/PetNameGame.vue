<template>
  <div class="pet-game-container p-6 max-w-lg mx-auto bg-[var(--secondary-light)] rounded-xl shadow-md">
    <h2 class="text-2xl font-bold text-center mb-4">Petfinder Name Game</h2>

    <!-- Score and Streak Display -->
    <div class="text-center mb-4">
      <p class="text-lg">Score: <span class="font-bold">{{ score }}</span></p>
      <p class="text-sm">Streak: <span class="font-bold">{{ currentStreak }}</span> (Max: {{ maxStreak }})</p>
    </div>


    <!-- Pet image -->
    <div class="pet-image-container mb-6">
      <div v-if="loading" class="flex justify-center items-center h-64">
        <div class="text-center">
          <p class="text-gray-500 mb-2">Loading pet image...</p>
          <div class="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
        </div>
      </div>

      <!-- Image -->
      <img 
        v-else-if="currentPet && currentPet.photo_url_small" 
        :src="currentPet.photo_url_small" 
        :alt="hasGuessed ? currentPet.name : 'Mystery pet'" 
        class="w-full h-64 object-contain rounded-lg"
      >
      <!-- Placeholder if no image -->
      <div v-else class="flex justify-center items-center h-64 bg-gray-200 rounded-lg">
        <p class="text-gray-500">No image available</p>
      </div>
    </div>

    <!-- Feedback Message Overlay (Correct/Incorrect/Skipped) -->
    <div 
      v-if="feedback" 
      :class="[
        'absolute inset-0 flex items-center justify-center p-4 text-center font-bold bg-black bg-opacity-10 rounded-lg pointer-events-none', 
        feedbackClass, 
        { 'text-4xl': guessResult !== 'skipped', 'text-xl': guessResult === 'skipped' } // Larger text for Correct/Wrong
      ]"
    >
      {{ feedback }}
    </div>

    <!-- Pet information (shown after guessing) -->
    <div v-if="hasGuessed && currentPet" class="mb-6 p-4 bg-gray-100 rounded-lg">
      <h3 
        :class="[
          'font-bold text-lg mb-2', 
          { 'text-green-500': guessResult === 'correct', 'text-red-500': guessResult === 'incorrect' }
        ]"
      >
        {{ currentPet.name }}
      </h3>
      <!-- Updated Link Display -->
      <p>
        <span class="font-semibold text-sm">Link: </span> 
        <a :href="currentPet.url" target="_blank" rel="noopener noreferrer" class="text-blue-600 hover:underline break-all">
          {{ shortenedPetUrl }}
        </a>
      </p>
      <!-- <p v-if="currentPet.primary_breed"><span class="font-semibold text-sm">Breed:</span> {{ currentPet.primary_breed }}</p> -->
      <!-- <p><span class="font-semibold text-sm">Age:</span> {{ currentPet.age }}</p> -->
      <p v-if="currentPet.description" class="mt-2 text-sm">
        {{ truncateDescription(currentPet.description) }}
      </p>
    </div>

    <!-- Name options -->
    <div class="grid grid-cols-2 gap-4 mb-6">
      <button
        v-for="(name, index) in nameOptions"
        :key="index"
        @click="makeGuess(name)"
        :disabled="hasGuessed"
        class="py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)] disabled:opacity-50 transition"
      >
        {{ name }}
      </button>
    </div>

    <!-- Next/Restart button -->
    <div class="text-center">
      <button
        @click="nextPet"
        class="py-2 px-8 bg-green-500 text-white rounded hover:bg-green-600 transition"
      >
        {{ hasGuessed ? 'Next Pet' : 'Skip This Pet' }}
      </button>
    </div>
  </div>
</template>

<script>
export default {
  name: 'PetNameGame',
  data() {
    return {
      score: 0,
      currentStreak: 0,
      maxStreak: 0,
      hasGuessed: false,
      feedback: '',
      feedbackClass: '',
      loading: true,
      currentPet: null,
      nameOptions: [],
      gameBoards: [],
      currentBoardIndex: 0,
      currentPetIndex: 0,
      filters: {
        type: '',
        age: ''
      }
    };
  },
  computed: {
    shortenedPetUrl() {
      if (!this.currentPet || !this.currentPet.url) {
        return '';
      }
      try {
        const url = new URL(this.currentPet.url);
        // Remove 'www.' if present
        let hostname = url.hostname.replace(/^www\./, '');
        // Combine hostname and pathname, truncate if too long
        let displayUrl = hostname + (url.pathname === '/' ? '' : url.pathname);
        const maxLength = 30; // Adjust max length as needed
        if (displayUrl.length > maxLength) {
          displayUrl = displayUrl.substring(0, maxLength) + '...';
        }
        return displayUrl;
      } catch (e) {
        // Fallback for invalid URLs
        return this.currentPet.url.substring(0, 30) + (this.currentPet.url.length > 30 ? '...' : '');
      }
    }
  },
  mounted() {
    this.fetchGameBoards();
  },
  methods: {
    async fetchGameBoards() {
      this.loading = true;
      this.feedback = '';
      
      try {
        const endpoint = `http://localhost:8000/api/petfinder/pets/game_boards`;
        console.log("Fetching from:", endpoint);

        const response = await fetch(endpoint);
        console.log("Raw response:", response);

        const data = await response.json();
        console.log("Fetched data:", data);
        
        if (data.data && data.data.length > 0) {
          this.gameBoards = data.data;
          console.log("Assigned gameBoards:", this.gameBoards);

          this.currentBoardIndex = 0;
          this.currentPetIndex = 0;
          this.loadCurrentPet();
        } else {
          this.feedback = 'No pets found with the current filters. Please try different options.';
          this.feedbackClass = 'text-red-600 font-bold';
          this.loading = false;
        }
      } catch (error) {
        console.error('Error fetching game boards:', error);
        this.feedback = 'Error loading pets. Please try again later.';
        this.feedbackClass = 'text-red-600 font-bold';
        this.loading = false;
      }
    },
    
    loadCurrentPet() {
      this.loading = true;
      this.hasGuessed = false;
      this.feedback = '';
      this.guessResult = null; // Reset guess result for the new pet
      
      // Check if we have a current game board
      const currentBoard = this.gameBoards[this.currentBoardIndex];
      if (!currentBoard || !currentBoard.game_board) {
        this.fetchGameBoards();
        return;
      }

      // Get the current pet from the game_board array
      const pet = currentBoard.game_board[this.currentPetIndex];

      if (!pet) {
        // Move to next board or fetch new boards if at the end
        if (this.currentBoardIndex < this.gameBoards.length - 1) {
          this.currentBoardIndex++;
          this.currentPetIndex = 0;
          this.loadCurrentPet();
        } else {
          this.fetchGameBoards();
        }
        return;
      }

      // Clean the name before assigning
      this.currentPet = { ...pet, name: this.cleanPetName(pet.name) };

      // Generate name options
      this.generateNameOptions();
      this.loading = false;
    },
    
    generateNameOptions() {
      if (!this.currentPet) return;
      
      // Always include the cleaned correct name
      const correctCleanedName = this.currentPet.name; // Already cleaned in loadCurrentPet
      const options = [correctCleanedName];
      
      // Add cleaned random names from other pets
      const allPetNames = this.getAllPetNames(); // Names are cleaned in getAllPetNames
      
      while (options.length < 4 && allPetNames.length > 0) {
        const randomIndex = Math.floor(Math.random() * allPetNames.length);
        const randomName = allPetNames[randomIndex];
        
        if (!options.includes(randomName) && randomName !== correctCleanedName) {
          options.push(randomName);
        }
        
        // Remove the name to avoid duplicates
        allPetNames.splice(randomIndex, 1);
      }
      
      // If we don't have enough names, generate some common pet names
      const commonPetNames = ['Max', 'Bella', 'Luna', 'Charlie', 'Lucy', 'Cooper', 
                             'Bailey', 'Daisy', 'Sadie', 'Lola', 'Buddy', 'Molly', 
                             'Stella', 'Tucker', 'Bentley', 'Zoey', 'Harley', 'Rocky'];
      
      while (options.length < 4) {
        const randomName = commonPetNames[Math.floor(Math.random() * commonPetNames.length)];
        if (!options.includes(randomName)) {
          options.push(randomName);
        }
      }
      
      // Shuffle options
      this.nameOptions = this.shuffleArray(options);
    },
    
    getAllPetNames() {
      // Collect and clean all pet names from all game boards
      const names = new Set(); // Use a Set to avoid duplicates initially
      this.gameBoards.forEach(board => {
        if (board.game_board) {
          board.game_board.forEach(pet => {
            if (pet && pet.name) {
              const cleanedName = this.cleanPetName(pet.name);
              // Add only if it's a valid name and not the current pet's cleaned name
              if (cleanedName && (!this.currentPet || cleanedName !== this.currentPet.name)) {
                 names.add(cleanedName);
              }
            }
          });
        }
      });
      return Array.from(names); // Convert Set back to Array
    },

    cleanPetName(name) {
      if (!name) return '';
      let cleaned = name;

      // Attempt to split by " - " and take the first part
      const parts = cleaned.split(' - ');
      cleaned = parts[0]; 

      // Remove common patterns like (see desc.), (A...), etc.
      cleaned = cleaned.replace(/\(.*?\)/g, '');

      // Remove leading/trailing non-alphanumeric chars (except spaces)
      cleaned = cleaned.replace(/^[^a-zA-Z0-9]+|[^a-zA-Z0-9\s]+$/g, '');

      // Remove specific unwanted characters like * / \
      cleaned = cleaned.replace(/[*\\/]/g, '');

      // Replace multiple spaces with a single space
      cleaned = cleaned.replace(/\s+/g, ' ');

      // Trim whitespace
      cleaned = cleaned.trim();

      // Optional: Capitalize first letter of each word (Title Case)
      // cleaned = cleaned.toLowerCase().split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');

      return cleaned;
    },

    shuffleArray(array) {
      const newArray = [...array];
      for (let i = newArray.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [newArray[i], newArray[j]] = [newArray[j], newArray[i]];
      }
      return newArray;
    },
    
    makeGuess(name) {
      if (!this.currentPet) return;
      this.hasGuessed = true;

      if (name === this.currentPet.name) {
        this.feedback = 'Correct!'; // Set overlay text
        this.feedbackClass = 'text-green-500'; // Set overlay color class
        this.guessResult = 'correct'; // Set state for name color in info box
        this.score += 1;
        this.currentStreak++;
        if (this.currentStreak > this.maxStreak) {
          this.maxStreak = this.currentStreak;
        }
      } else {
        this.feedback = 'WRONG!'; // Set overlay text
        this.feedbackClass = 'text-red-500'; // Set overlay color class
        this.guessResult = 'incorrect'; // Set state for name color in info box
        this.currentStreak = 0;
      }
    },
    
    nextPet() {
      if (!this.hasGuessed) { // Handle skip
        this.currentStreak = 0;
        this.feedback = `Skipped! The name was ${this.currentPet?.name || 'unknown'}.`; 
        this.feedbackClass = 'text-orange-500'; 
        this.guessResult = 'skipped'; 
      } 
      // No need for an else block, loadCurrentPet clears feedback for the next round

      this.currentPetIndex++;

      const currentBoard = this.gameBoards[this.currentBoardIndex];
      if (!currentBoard || !currentBoard.game_board) {
        this.fetchGameBoards(); // Fetch if board data is missing
        return;
      }

      const maxPets = currentBoard.game_board.length;

      if (this.currentPetIndex >= maxPets) {
        this.currentPetIndex = 0;
        this.currentBoardIndex++;

        if (this.currentBoardIndex >= this.gameBoards.length) {
          // Optionally show a "Game Over" or fetch new boards
          this.fetchGameBoards(); // Fetch new boards for now
          return;
        }
      }

      this.loadCurrentPet(); // This will load the next pet and clear feedback
    },
    
    applyFilters() {
      this.score = 0; // Reset score when applying new filters
      this.currentStreak = 0; // Reset streak too
      this.maxStreak = 0;
      this.fetchGameBoards();
    },
    
    truncateDescription(description) {
      if (!description) return '';
      return description.length > 150 ? description.substring(0, 150) + '...' : description;
    }
  }
};
</script>

<style scoped>
.pet-game-container {
  transition: all 0.3s ease;
}
/* Ensure feedback text color classes override others if needed */
.text-green-500 { color: #16a34a !important; } /* Example green */
.text-red-500 { color: #dc2626 !important; } /* Example red */
.text-orange-500 { color: #ea580c !important; } /* Example orange */
</style>
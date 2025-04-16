<template>
  <div class="pet-game-container p-6 max-w-lg mx-auto bg-[var(--secondary-light)] rounded-xl shadow-md">
    <h2 class="text-2xl font-bold text-center mb-4">Petfinder Name Game</h2>

    <!-- Score and Streak Display -->
    <div class="text-center mb-4">
      <p class="text-lg">Score: <span class="font-bold">{{ score }}</span></p>
      <p class="text-sm">Current Streak: <span class="font-bold">{{ currentStreak }}</span> (Max: {{ maxStreak }})</p>
    </div>


    <!-- Pet image -->
    <div class="pet-image-container mb-6">
      <div v-if="loading" class="flex justify-center items-center h-64">
        <div class="text-center">
          <p class="text-gray-500 mb-2">Loading pet image...</p>
          <div class="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
        </div>
      </div>
      <img 
        v-else-if="currentPet && currentPet.photo_url_small" 
        :src="currentPet.photo_url_small" 
        :alt="hasGuessed ? currentPet.name : 'Mystery pet'" 
        class="w-full h-64 object-cover rounded-lg"
      >
      <div v-else class="flex justify-center items-center h-64 bg-gray-200 rounded-lg">
        <p class="text-gray-500">No image available</p>
      </div>
    </div>

    <!-- Feedback Message -->
    <div v-if="feedback" :class="['text-center mb-4 font-semibold', feedbackClass]">
      {{ feedback }}
    </div>

    <!-- Pet information (shown after guessing) -->
    <div v-if="hasGuessed && currentPet" class="mb-6 p-4 bg-gray-100 rounded-lg">
      <h3 class="font-bold text-lg mb-2">{{ currentPet.name }}</h3>
      <p><span class="font-semibold">Type:</span> {{ currentPet.type }}</p>
      <p v-if="currentPet.primary_breed"><span class="font-semibold">Breed:</span> {{ currentPet.primary_breed }}</p>
      <p><span class="font-semibold">Age:</span> {{ currentPet.age }}</p>
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
      
      // Check if we have a current game board
      const currentBoard = this.gameBoards[this.currentBoardIndex];
      if (!currentBoard || !currentBoard.game_board) {
        this.fetchGameBoards();
        return;
      }

      // Get the current pet from the game_board array
      const pet = currentBoard.game_board[this.currentPetIndex];

      console.log("Current pet index:", this.currentPetIndex);
      console.log("Current pet object:", pet);

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

      this.currentPet = pet;

      // Generate name options
      this.generateNameOptions();
      this.loading = false;
    },
    
    generateNameOptions() {
      if (!this.currentPet) return;
      
      // Always include the correct name
      const options = [this.currentPet.name];
      
      // Add random names from other pets in different game boards
      const allPetNames = this.getAllPetNames();
      
      while (options.length < 4 && allPetNames.length > 0) {
        const randomIndex = Math.floor(Math.random() * allPetNames.length);
        const randomName = allPetNames[randomIndex];
        
        if (!options.includes(randomName) && randomName !== this.currentPet.name) {
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
      // Collect all pet names from all game boards
      const names = [];
      this.gameBoards.forEach(board => {
        if (board.game_board) {
          board.game_board.forEach(pet => {
            if (pet && pet.name && (!this.currentPet || pet.name !== this.currentPet.name)) {
              names.push(pet.name);
            }
          });
        }
      });
      return names;
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
      if (!this.currentPet) return; // Ensure currentPet is loaded
      this.hasGuessed = true;

      if (name === this.currentPet.name) {
        this.feedback = `Correct! This pet is indeed named ${name}.`;
        this.feedbackClass = 'text-green-600';
        this.score += 10;
        this.currentStreak++;
        if (this.currentStreak > this.maxStreak) {
          this.maxStreak = this.currentStreak;
        }
      } else {
        this.feedback = `Incorrect. The pet's name is ${this.currentPet.name}.`;
        this.feedbackClass = 'text-red-600';
        this.currentStreak = 0;
      }
    },
    
    nextPet() {
      // If skipping (hasGuessed is false), reset the streak
      if (!this.hasGuessed) {
        this.currentStreak = 0;
        this.feedback = `Skipped. The pet's name was ${this.currentPet?.name || 'unknown'}.`; // Optional feedback for skip
        this.feedbackClass = 'text-orange-600';
      } else {
         // Clear feedback for the next round only if they guessed
         this.feedback = '';
         this.feedbackClass = '';
      }


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

      this.loadCurrentPet();
    },
    
    applyFilters() {
      this.score = 0; // Reset score when applying new filters
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
</style>
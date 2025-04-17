<template>
  <div class="home flex flex-col items-center">
    <!-- Games Wrapper -->
    <div class="flex flex-wrap justify-center gap-6 w-full max-w-4xl">
      <!-- Game Card for Pet Name Guessing -->
      <div class="game-card bg-[var(--secondary-light)] p-6 rounded-xl shadow-md hover:shadow-lg transition max-w-md w-full">
        <h3 class="text-xl font-bold mb-2 text-center">Petfinder Name Game</h3>
        <!-- <p class="mb-4 text-center">Guess the names of these adoptable pets near you?</p> -->

        <div class="w-full h-48 mb-4 rounded-lg overflow-hidden bg-gray-200 flex items-center justify-center"> 
          <img 
            src="../assets/girl_glare_cropped.png" 
            alt="Pet Name Game Preview" 
            class="max-w-full max-h-full object-contain rounded-lg mb-4" 
          />
        </div>

        <router-link 
          to="/games/pet-name-game" 
          class="block text-center py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)]"
        >
          Play Now
        </router-link>
      </div>

      <!-- Additional Game Cards -->
    </div>
  </div>
</template>

<script>
export default {
  name: 'Home'
}
</script>
